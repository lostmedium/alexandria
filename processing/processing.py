import base64
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from urllib.parse import unquote_plus

import boto3
import coordination
import pymupdf
import pymupdf4llm
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client("s3", config=Config(max_pool_connections=20))
sqs = boto3.client("sqs")
lambda_client = boto3.client(
    "lambda", config=Config(read_timeout=900, retries={"total_max_attempts": 1}),
)
PAGE_LIMIT = 50
CHUNK_LIMIT = 15000
WORK_DIR = Path(tempfile.gettempdir()) / "alexandria-processing"
pymupdf4llm.use_layout(False)

# Lambda runtime resets can retain files from a timed-out invocation.
if WORK_DIR.exists():
    shutil.rmtree(WORK_DIR)


@contextmanager
def workdir():
    WORK_DIR.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=WORK_DIR) as directory:
        yield Path(directory)


class ProcessingPending(RuntimeError):
    pass


def queue_handler(handler):
    @wraps(handler)
    def handle(event, context):
        failures = []
        for message in event["Records"]:
            try:
                handler({"Records": [message]}, context)
            except (coordination.LockBusy, ProcessingPending) as error:
                logger.info("Deferring %s: %s", message["messageId"], error)
                sqs.change_message_visibility(
                    QueueUrl=os.environ["QUEUE_URL"],
                    ReceiptHandle=message["receiptHandle"], VisibilityTimeout=30,
                )
                failures.append({"itemIdentifier": message["messageId"]})
        return {"batchItemFailures": failures}
    return handle


def events(event):
    for message in event["Records"]:
        body = json.loads(message["body"])
        if body.get("Event") == "s3:TestEvent":
            continue
        for record in body["Records"]:
            yield record["s3"]["bucket"]["name"], unquote_plus(
                record["s3"]["object"]["key"]
            ), record["s3"]["object"]


def download(bucket, key, path, *, etag=None, version=None):
    args = {"Bucket": bucket, "Key": key}
    if etag:
        args["IfMatch"] = etag
    if version:
        args["VersionId"] = version
    response = s3.get_object(**args)
    try:
        with open(path, "wb") as target:
            shutil.copyfileobj(response["Body"], target, length=1024 * 1024)
    finally:
        response["Body"].close()
    return response.get("Metadata", {})


def current_version(bucket, key):
    return s3.head_object(Bucket=bucket, Key=key).get("VersionId", "null")


def cleanup(key):
    result = lambda_client.invoke(
        FunctionName=os.environ["CLEANUP_FUNCTION_NAME"], InvocationType="RequestResponse",
        Payload=json.dumps({"key": key}).encode("utf-8"),
    )
    try:
        payload = result["Payload"].read()
    finally:
        result["Payload"].close()
    if result.get("FunctionError"):
        raise RuntimeError(f"Cleanup failed for {key}: {payload.decode('utf-8')}")


@queue_handler
def split_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".pdf"):
            continue
        etag = obj.get("eTag")
        if not etag:
            raise ValueError(f"Missing ETag for {key}")
        version = obj.get("versionId") or "null"
        filename = key.rsplit("/", 1)[-1][:-4]
        source_id = coordination.source_id(key)
        generation = coordination.generation_id(f"{version}:{uuid.uuid4()}")
        metadata = {
            "source-key": base64.b64encode(key.encode()).decode(),
            "source-version": base64.b64encode(version.encode()).decode(),
            "generation": generation,
        }
        with coordination.locked(f"{source_id}:split", wait_seconds=0):
            with coordination.locked(source_id):
                if current_version(bucket, key) != version:
                    logger.info("Skipping stale PDF event for %s", key)
                    continue
                state = coordination.state(source_id)
                if state and state.get("status") == "blocked" and state.get("version") == version:
                    logger.info("Skipping blocked PDF version for %s", key)
                    continue
                if state and state.get("status") == "ready" and state.get("version") == version:
                    publish_completion(source_id, state)
                    logger.info("Already split %s", key)
                    continue
                coordination.put_state(
                    source_id, status="preparing", source_key=key,
                    version=version, generation=generation,
                )
                coordination.clear_parts(source_id)
                cleanup(key)
            with workdir() as directory:
                source = directory / "source.pdf"
                download(bucket, key, source, etag=etag, version=version)
                with source.open("rb") as file:
                    revision_id = hashlib.file_digest(file, "sha256").hexdigest()
                with pymupdf.open(source) as document:
                    if document.page_count == 0:
                        raise ValueError(f"PDF has no pages: {key}")
                    expected = (document.page_count + PAGE_LIMIT - 1) // PAGE_LIMIT
                    with coordination.locked(source_id):
                        state = coordination.state(source_id)
                        if not state or state.get("generation") != generation or \
                                current_version(bucket, key) != version:
                            logger.info("Discarding superseded split %s", key)
                            continue
                        coordination.put_state(
                            source_id, status="splitting", source_key=key, version=version,
                            generation=generation, revision=revision_id, expected=expected,
                        )
                    for index, start in enumerate(range(0, document.page_count, PAGE_LIMIT), 1):
                        part_path = directory / "part.pdf"
                        with pymupdf.open() as part:
                            part.insert_pdf(
                                document, from_page=start,
                                to_page=min(start + PAGE_LIMIT, document.page_count) - 1,
                            )
                            part.save(part_path)
                        with coordination.locked(source_id):
                            state = coordination.state(source_id)
                            if not state or state.get("generation") != generation or \
                                    current_version(bucket, key) != version:
                                logger.info("Discarding superseded split %s", key)
                                break
                            s3.upload_file(
                                str(part_path), destination,
                                f"{filename}/source-{source_id}__revision-{revision_id}"
                                f"__generation-{generation}__part-{index:06d}.pdf",
                                ExtraArgs={"ContentType": "application/pdf", "Metadata": metadata},
                            )
                        part_path.unlink()
                    else:
                        with coordination.locked(source_id):
                            state = coordination.state(source_id)
                            if not state or state.get("generation") != generation or \
                                    current_version(bucket, key) != version:
                                logger.info("Discarding superseded split %s", key)
                                continue
                            coordination.put_state(
                                source_id, status="ready", source_key=key, version=version,
                                generation=generation, revision=revision_id, expected=expected,
                            )
                            publish_completion(source_id, {
                                **state, "status": "ready",
                            })
                        logger.info("Split %s into revision %s", key, revision_id)


def source_for_part(key):
    match = re.match(
        r"^source-([0-9a-f]{64})__revision-[0-9a-f]{64}"
        r"__generation-([0-9a-f]{64})__part-(\d{6,})",
        key.rsplit("/", 1)[-1],
    )
    if not match:
        return None
    return match.groups()


def valid_part(bucket, key, obj, source, generation):
    state = coordination.state(source)
    if state and state.get("status") == "preparing" and state.get("generation") == generation:
        raise ProcessingPending(f"PDF split is not ready: {source}")
    if not state or state.get("status") not in ("splitting", "ready") or \
            state.get("generation") != generation:
        return False
    if current_version(os.environ["SOURCE_BUCKET"], state["source_key"]) != state["version"]:
        return False
    try:
        version = current_version(bucket, key)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return version == (obj.get("versionId") or "null")


def publish_completion(source, state):
    # Call under the source lease, including when the last chunk precedes split completion.
    generation = state["generation"]
    if state["status"] == "ready" and \
            coordination.completed_parts(source, generation) == state["expected"]:
        s3.put_object(
            Bucket=os.environ["CHUNKS_BUCKET"],
            Key=f"__completion__/{source}/{generation}.json",
            Body=json.dumps({
                "source_key": state["source_key"], "source_id": source,
                "generation": generation, "version": state["version"],
                "revision": state["revision"],
            }).encode("utf-8"),
            ContentType="application/json",
        )


def already_converted(destination, key, version):
    try:
        head = s3.head_object(Bucket=destination, Key=key[:-4] + ".md")
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return head.get("Metadata", {}).get("part-version") == version


@queue_handler
def convert_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".pdf"):
            continue
        identity = source_for_part(key)
        if not identity:
            logger.info("Skipping legacy part event %s", key)
            continue
        source_id, generation, part = identity
        logger.info("Converting %s", key)
        with coordination.locked(
            f"{source_id}:convert:{generation}:{part}", wait_seconds=0,
        ):
            if not valid_part(bucket, key, obj, source_id, generation):
                logger.info("Skipping stale part event %s", key)
                continue
            version = obj.get("versionId") or "null"
            if already_converted(destination, key, version):
                logger.info("Already converted %s", key)
                continue
            with workdir() as directory:
                source = directory / "part.pdf"
                metadata = download(bucket, key, source, version=obj.get("versionId"))
                with pymupdf.open(source) as document:
                    text = pymupdf4llm.to_markdown(document, show_progress=False)
                if not text.strip():
                    raise ValueError(f"No Markdown extracted from {key}")
            with coordination.locked(source_id, wait_seconds=2):
                if not valid_part(bucket, key, obj, source_id, generation):
                    logger.info("Discarding superseded conversion %s", key)
                    continue
                s3.put_object(
                    Bucket=destination, Key=key[:-4] + ".md", Body=text.encode("utf-8"),
                    ContentType="text/markdown; charset=utf-8",
                    Metadata={**metadata, "part-version": version},
                )
        logger.info("Converted %s", key)


def _blocks(text, pattern):
    blocks, current = [], []
    fence = None
    for line in text.splitlines(keepends=True):
        match = re.match(r"^\s*(`{3,}|~{3,})", line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
        if fence is None and re.match(pattern, line) and current:
            blocks.append("".join(current))
            current = []
        current.append(line)
    if current:
        blocks.append("".join(current))
    return blocks


def _atoms(text):
    atoms, current = [], []
    fence = None
    table = False
    for line in text.splitlines(keepends=True):
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        is_table = line.lstrip().startswith("|")
        if fence:
            current.append(line)
            if marker and marker.group(1)[0] == fence[0] and len(marker.group(1)) >= len(fence):
                atoms.append("".join(current))
                current, fence = [], None
        elif marker:
            if current:
                atoms.append("".join(current))
            current, fence, table = [line], marker.group(1), False
        elif is_table:
            if current and not table:
                atoms.append("".join(current))
                current = []
            current.append(line)
            table = True
        else:
            if current:
                atoms.append("".join(current))
            current, table = [line], False
    if current:
        atoms.append("".join(current))
    return atoms


def _pack(pieces):
    result, current = [], ""
    for piece in pieces:
        if current and len(current) + len(piece) > CHUNK_LIMIT:
            result.append(current)
            current = ""
        current += piece
    if current:
        result.append(current)
    return result


def _paragraphs(text):
    paragraphs, current = [], []
    for atom in _atoms(text):
        current.append(atom)
        if not atom.strip():
            paragraphs.append("".join(current))
            current = []
    if current:
        paragraphs.append("".join(current))
    return paragraphs


def _fit(text, level=0):
    if len(text) <= CHUNK_LIMIT:
        return [text]
    splitters = (
        lambda value: _blocks(value, r"^#{3,6}\s"),
        _paragraphs,
        _atoms,
        lambda value: [value[i:i + CHUNK_LIMIT] for i in range(0, len(value), CHUNK_LIMIT)],
    )
    if level == len(splitters):
        raise ValueError("Unable to split Markdown")
    pieces = splitters[level](text)
    return [
        chunk
        for piece in _pack(pieces)
        for chunk in _fit(piece, level + 1)
    ]


def split_markdown(text):
    result = []
    for section in _pack(_blocks(text, r"^#{1,2}\s")):
        heading = section.splitlines()[0] if re.match(r"^#{1,2}\s", section) else ""
        for index, chunk in enumerate(_fit(section)):
            if index and heading:
                chunk = f"{heading} (continued)\n\n{chunk}"
            if chunk.strip():
                result.append(chunk)
    return result


@queue_handler
def chunk_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".md"):
            continue
        identity = source_for_part(key)
        if not identity:
            logger.info("Skipping legacy Markdown event %s", key)
            continue
        source_id, generation, part = identity
        if not valid_part(bucket, key, obj, source_id, generation):
            logger.info("Skipping stale Markdown event %s", key)
            continue
        with workdir() as directory:
            source = directory / "source.md"
            metadata = download(bucket, key, source, version=obj.get("versionId"))
            text = source.read_text(encoding="utf-8")
        chunks = split_markdown(text)
        if not chunks:
            raise ValueError(f"No chunks produced from {key}")
        source_key = base64.b64decode(metadata["source-key"], validate=True).decode("utf-8")
        title = next((line[2:].strip() for line in text.splitlines() if line.startswith("# ")), "")
        new_keys = set()
        uploads = []
        prefix = f"{key[:-3]}-chunk-"
        for number, chunk in enumerate(chunks, 1):
            heading = next(
                (line.lstrip("#").strip().removesuffix(" (continued)")
                 for line in chunk.splitlines() if re.match(r"^#{1,2}\s", line)), ""
            )
            preamble = [f"> Source: {source_key}", f"> Part: {key}"]
            if title:
                preamble.append(f"> Document: {title}")
            if heading and heading != title:
                preamble.append(f"> Section: {heading}")
            preamble.append(f"> Chunk {number} of {len(chunks)}")
            body = "\n".join(preamble) + "\n\n" + chunk
            encoded = body.encode("utf-8")
            digest = hashlib.sha256(encoded).hexdigest()
            chunk_key = f"{prefix}{number:06d}-{digest}.md"
            new_keys.add(chunk_key)
            uploads.append((chunk_key, encoded, digest))

        def upload(item):
            chunk_key, encoded, digest = item
            return s3.put_object(
                Bucket=destination, Key=chunk_key, Body=encoded,
                ContentType="text/markdown; charset=utf-8", Metadata={"sha256": digest},
            )

        with coordination.locked(source_id, wait_seconds=2):
            if not valid_part(bucket, key, obj, source_id, generation):
                logger.info("Discarding superseded chunks %s", key)
                continue
            state = coordination.state(source_id)
            if not 1 <= int(part) <= state["expected"]:
                raise ValueError(f"Unexpected part number in {key}")
            coordination.clear_part(source_id, generation, part)
            existing = {
                item["Key"]
                for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=destination, Prefix=prefix,
                )
                for item in page.get("Contents", [])
            }
            with ThreadPoolExecutor(max_workers=16) as executor:
                for _ in executor.map(upload, (item for item in uploads if item[0] not in existing)):
                    pass
            obsolete = [{"Key": old_key} for old_key in existing - new_keys]
            for start in range(0, len(obsolete), 1000):
                result = s3.delete_objects(
                    Bucket=destination,
                    Delete={"Objects": obsolete[start:start + 1000], "Quiet": True},
                )
                if result.get("Errors"):
                    raise RuntimeError(f"Failed to delete old chunks: {result['Errors']}")
            coordination.mark_part(source_id, generation, part, len(new_keys))
            publish_completion(source_id, state)
        logger.info("Chunked %s into %d chunks", key, len(chunks))

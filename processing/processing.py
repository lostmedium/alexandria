import base64
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote_plus

import boto3
import pymupdf
import pymupdf4llm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
PAGE_LIMIT = 1000
CHUNK_LIMIT = 15000
WORK_DIR = Path(tempfile.gettempdir()) / "alexandria-processing"


@contextmanager
def workdir():
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir()
    try:
        yield WORK_DIR
    finally:
        shutil.rmtree(WORK_DIR)


def events(event):
    for message in event["Records"]:
        body = json.loads(message["body"])
        if body.get("Event") == "s3:TestEvent":
            continue
        for record in body["Records"]:
            yield record["s3"]["bucket"]["name"], unquote_plus(
                record["s3"]["object"]["key"]
            ), record["s3"]["object"]


def download(bucket, key, path, *, etag=None):
    args = {"Bucket": bucket, "Key": key}
    if etag:
        args["IfMatch"] = etag
    response = s3.get_object(**args)
    try:
        with open(path, "wb") as target:
            shutil.copyfileobj(response["Body"], target)
    finally:
        response["Body"].close()
    return response.get("Metadata", {})


def split_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".pdf"):
            continue
        etag = obj.get("eTag")
        if not etag:
            raise ValueError(f"Missing ETag for {key}")
        revision_id = hashlib.sha256(etag.encode()).hexdigest()
        filename = key.rsplit("/", 1)[-1][:-4]
        source_id = hashlib.sha256(key.encode()).hexdigest()
        metadata = {"source-key": base64.b64encode(key.encode()).decode()}
        with workdir() as directory:
            source = directory / "source.pdf"
            download(bucket, key, source, etag=etag)
            with pymupdf.open(source) as document:
                if document.page_count == 0:
                    raise ValueError(f"PDF has no pages: {key}")
                for index, start in enumerate(range(0, document.page_count, PAGE_LIMIT), 1):
                    part_path = directory / "part.pdf"
                    with pymupdf.open() as part:
                        part.insert_pdf(
                            document, from_page=start,
                            to_page=min(start + PAGE_LIMIT, document.page_count) - 1,
                        )
                        part.save(part_path, garbage=4, deflate=True)
                    s3.upload_file(
                        str(part_path), destination,
                        f"{filename}/source-{source_id}__revision-{revision_id}"
                        f"__part-{index:06d}.pdf",
                        ExtraArgs={"ContentType": "application/pdf", "Metadata": metadata},
                    )
                    part_path.unlink()
        logger.info("Split %s into revision %s", key, revision_id)


def convert_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".pdf"):
            continue
        logger.info("Converting %s", key)
        with workdir() as directory:
            source = directory / "part.pdf"
            metadata = download(bucket, key, source)
            with pymupdf.open(source) as document:
                text = pymupdf4llm.to_markdown(document, force_ocr=False)
            if not text.strip():
                raise ValueError(f"No Markdown extracted from {key}")
            s3.put_object(
                Bucket=destination, Key=key[:-4] + ".md", Body=text.encode("utf-8"),
                ContentType="text/markdown; charset=utf-8", Metadata=metadata,
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


def chunk_handler(event, context):
    destination = os.environ["DESTINATION_BUCKET"]
    for bucket, key, obj in events(event):
        if not key.lower().endswith(".md"):
            continue
        with workdir() as directory:
            source = directory / "source.md"
            metadata = download(bucket, key, source)
            text = source.read_text(encoding="utf-8")
        chunks = split_markdown(text)
        if not chunks:
            raise ValueError(f"No chunks produced from {key}")
        source_key = base64.b64decode(metadata["source-key"], validate=True).decode("utf-8")
        title = next((line[2:].strip() for line in text.splitlines() if line.startswith("# ")), "")
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
            s3.put_object(
                Bucket=destination, Key=f"{key[:-3]}-chunk-{number:06d}.md",
                Body=body.encode("utf-8"), ContentType="text/markdown; charset=utf-8",
            )
        logger.info("Chunked %s into %d chunks", key, len(chunks))

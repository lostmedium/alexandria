import hashlib
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from itertools import islice
from urllib.parse import unquote_plus

import boto3
import coordination
from botocore.config import Config
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client("s3", config=Config(max_pool_connections=20))


def keys(bucket, prefix):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def checksum(bucket, key, head):
    digest = head.get("Metadata", {}).get("sha256")
    if digest and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest):
        return digest
    args = {"Bucket": bucket, "Key": key}
    if head.get("VersionId"):
        args["VersionId"] = head["VersionId"]
    body = s3.get_object(**args)["Body"]
    try:
        hasher = hashlib.sha256()
        while data := body.read(1024 * 1024):
            hasher.update(data)
        return hasher.hexdigest()
    finally:
        body.close()


def sync_object(source, destination, key):
    head = s3.head_object(Bucket=source, Key=key)
    digest = checksum(source, key, head)
    try:
        staged = s3.head_object(Bucket=destination, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
        staged = None
    if staged and checksum(destination, key, staged) == digest:
        version = staged.get("VersionId")
    else:
        copy_source = {"Bucket": source, "Key": key}
        if head.get("VersionId"):
            copy_source["VersionId"] = head["VersionId"]
        args = {
            "Bucket": destination, "Key": key, "CopySource": copy_source,
            "CopySourceIfMatch": head["ETag"], "MetadataDirective": "REPLACE",
            "Metadata": {**head.get("Metadata", {}), "sha256": digest},
        }
        for field in ("ContentType", "ContentEncoding", "ContentLanguage",
                      "ContentDisposition", "CacheControl", "Expires",
                      "WebsiteRedirectLocation"):
            if field in head:
                args[field] = head[field]
        version = s3.copy_object(**args).get("VersionId")
    if not version:
        raise RuntimeError(f"Staged object has no version ID: {key}")
    return {"key": key, "version_id": version, "sha256": digest}


def delete_batch(bucket, objects):
    result = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
    if result.get("Errors"):
        raise RuntimeError(f"Failed to delete staging objects: {result['Errors']}")


def stage(source_id, generation, marker_version=None):
    chunks_bucket = os.environ["CHUNKS_BUCKET"]
    staging_bucket = os.environ["STAGING_BUCKET"]
    source_bucket = os.environ["SOURCE_BUCKET"]
    if chunks_bucket == staging_bucket:
        raise ValueError("Chunks and staging buckets must be different")

    marker_key = f"__completion__/{source_id}/{generation}.json"
    with coordination.locked(source_id):
        state = coordination.state(source_id)
        if not state or state.get("status") != "ready" or state.get("generation") != generation:
            logger.info("Skipping stale completion for %s", source_id)
            return False
        if coordination.completed_parts(source_id, generation) != state["expected"]:
            logger.info("Skipping incomplete revision for %s", source_id)
            return False
        if s3.head_object(Bucket=source_bucket, Key=state["source_key"]).get(
            "VersionId", "null"
        ) != state["version"]:
            logger.info("Skipping superseded completion for %s", source_id)
            return False
        try:
            marker_head = s3.head_object(Bucket=chunks_bucket, Key=marker_key)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                logger.info("Skipping deleted completion marker %s", marker_key)
                return False
            raise
        if marker_version and marker_head.get("VersionId", "null") != marker_version:
            logger.info("Skipping superseded completion marker %s", marker_key)
            return False

        filename = state["source_key"].rsplit("/", 1)[-1][:-4]
        source_prefix = f"{filename}/source-{source_id}__"
        revision_prefix = (
            f"{source_prefix}revision-{state['revision']}__generation-{generation}__"
        )
        source_keys = [key for key in keys(chunks_bucket, revision_prefix) if key.endswith(".md")]
        counts = coordination.completed_chunk_counts(source_id, generation)
        actual = {}
        for key in source_keys:
            match = re.fullmatch(
                re.escape(revision_prefix) + r"part-(\d{6,})-chunk-(\d{6,})-[0-9a-f]{64}\.md",
                key,
            )
            if not match:
                raise ValueError(f"Unexpected chunk key: {key}")
            part, number = match.groups()
            actual.setdefault(part, []).append(int(number))
        expected_parts = {f"{part:06d}" for part in range(1, int(state["expected"]) + 1)}
        if set(counts) != expected_parts or set(actual) != expected_parts or any(
            not isinstance(counts[part], (int, Decimal)) or counts[part] <= 0 or
            counts[part] != int(counts[part]) or
            sorted(actual[part]) != list(range(1, int(counts[part]) + 1))
            for part in expected_parts
        ):
            raise ValueError(f"Missing or unexpected chunks for completed PDF {state['source_key']}")
        source_keys = iter(source_keys)
        entries = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            while batch := list(islice(source_keys, 100)):
                entries.extend(executor.map(
                    lambda key: sync_object(chunks_bucket, staging_bucket, key), batch,
                ))
        if not entries:
            raise ValueError(f"No chunks found for completed PDF {state['source_key']}")
        entries.sort(key=lambda item: item["key"])
        manifest_key = f"{source_prefix}manifest.json"
        manifest = {
            "source_key": state["source_key"], "source_version": state["version"],
            "generation": generation, "revision": state["revision"], "chunks": entries,
        }
        s3.put_object(
            Bucket=staging_bucket, Key=manifest_key,
            Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        active = {item["key"] for item in entries}
        batch = []
        for key in keys(staging_bucket, source_prefix):
            if key != manifest_key and key not in active:
                batch.append({"Key": key})
                if len(batch) == 1000:
                    delete_batch(staging_bucket, batch)
                    batch = []
        if batch:
            delete_batch(staging_bucket, batch)
        logger.info("Staged %d chunks for %s", len(entries), state["source_key"])
        return True


def lambda_handler(event, context):
    if "source_key" in event:
        source_id = coordination.source_id(event["source_key"])
        state = coordination.state(source_id)
        if not state or state.get("status") != "ready":
            raise ValueError(f"No completed revision for {event['source_key']}")
        if not stage(source_id, state["generation"]):
            raise RuntimeError(f"PDF revision is not ready for staging: {event['source_key']}")
        return {"source_key": event["source_key"], "status": "staged"}
    for message in event["Records"]:
        body = json.loads(message["body"])
        if body.get("Event") == "s3:TestEvent":
            continue
        for record in body["Records"]:
            obj = record["s3"]["object"]
            key = unquote_plus(obj["key"])
            parts = key.split("/")
            if len(parts) != 3 or parts[0] != "__completion__" or not parts[2].endswith(".json"):
                raise ValueError(f"Invalid completion marker key: {key}")
            stage(parts[1], parts[2][:-5], obj.get("versionId"))

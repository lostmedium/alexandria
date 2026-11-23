import hashlib
import logging
import os

import boto3


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")


def lambda_handler(event, context):
    key = event["key"]
    if not key.lower().endswith(".pdf"):
        raise ValueError(f"Not a PDF key: {key}")
    filename = key.rsplit("/", 1)[-1][:-4]
    source_id = hashlib.sha256(key.encode("utf-8")).hexdigest()
    prefix = f"{filename}/source-{source_id}__"
    for variable in ("PARTS_BUCKET", "MARKDOWN_BUCKET", "CHUNKS_BUCKET"):
        bucket = os.environ[variable]
        prefixes = [prefix]
        if variable == "CHUNKS_BUCKET":
            prefixes.append(f"__completion__/{source_id}/")
        for current_prefix in prefixes:
            pages = s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=current_prefix,
            )
            for page in pages:
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    result = s3.delete_objects(
                        Bucket=bucket, Delete={"Objects": objects, "Quiet": True},
                    )
                    if result.get("Errors"):
                        raise RuntimeError(f"Failed to delete objects in {bucket}: {result['Errors']}")
                    logger.info("Deleted %d objects from %s for %s", len(objects), bucket, key)
    return {"status": "ok"}

import hashlib
import os
import time
import uuid
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def source_id(key):
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def generation_id(version):
    return hashlib.sha256(version.encode("utf-8")).hexdigest()


def table():
    return boto3.resource("dynamodb").Table(os.environ["COORDINATION_TABLE"])


def state(source):
    return table().get_item(
        Key={"pk": source, "sk": "STATE"}, ConsistentRead=True,
    ).get("Item")


def put_state(source, **values):
    table().put_item(Item={"pk": source, "sk": "STATE", **values})


class LockBusy(RuntimeError):
    pass


@contextmanager
def locked(source, *, wait_seconds=30):
    owner = str(uuid.uuid4())
    deadline = time.monotonic() + wait_seconds
    while True:
        now = int(time.time())
        try:
            table().put_item(
                Item={"pk": source, "sk": "LOCK", "owner": owner, "expires": now + 960},
                ConditionExpression="attribute_not_exists(pk) OR #expires < :now",
                ExpressionAttributeNames={"#expires": "expires"},
                ExpressionAttributeValues={":now": now},
            )
            break
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            if time.monotonic() >= deadline:
                raise LockBusy(f"PDF processing is already in progress: {source}") from error
            time.sleep(0.25)
    try:
        yield
    finally:
        table().delete_item(
            Key={"pk": source, "sk": "LOCK"},
            ConditionExpression="#owner = :owner",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={":owner": owner},
        )


def mark_part(source, generation, part, chunk_count):
    table().put_item(Item={
        "pk": source, "sk": f"PART#{generation}#{part}", "chunk_count": chunk_count,
    })


def clear_part(source, generation, part):
    table().delete_item(Key={"pk": source, "sk": f"PART#{generation}#{part}"})


def clear_parts(source):
    current_table = table()
    args = {
        "KeyConditionExpression": Key("pk").eq(source) & Key("sk").begins_with("PART#"),
        "ConsistentRead": True,
        "ProjectionExpression": "pk, sk",
    }
    with current_table.batch_writer() as batch:
        while True:
            page = current_table.query(**args)
            for item in page["Items"]:
                batch.delete_item(Key=item)
            if "LastEvaluatedKey" not in page:
                return
            args["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def completed_parts(source, generation):
    args = {
        "KeyConditionExpression": Key("pk").eq(source) & Key("sk").begins_with(
            f"PART#{generation}#"
        ),
        "ConsistentRead": True,
        "Select": "COUNT",
    }
    count = 0
    while True:
        page = table().query(**args)
        count += page["Count"]
        if "LastEvaluatedKey" not in page:
            return count
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def completed_chunk_counts(source, generation):
    prefix = f"PART#{generation}#"
    args = {
        "KeyConditionExpression": Key("pk").eq(source) & Key("sk").begins_with(prefix),
        "ConsistentRead": True,
    }
    counts = {}
    while True:
        page = table().query(**args)
        for item in page["Items"]:
            counts[item["sk"][len(prefix):]] = item.get("chunk_count")
        if "LastEvaluatedKey" not in page:
            return counts
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

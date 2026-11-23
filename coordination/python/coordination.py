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


@contextmanager
def locked(source):
    owner = str(uuid.uuid4())
    deadline = time.monotonic() + 30
    while True:
        now = int(time.time())
        try:
            table().put_item(
                Item={"pk": source, "sk": "LOCK", "owner": owner, "expires": now + 1860},
                ConditionExpression="attribute_not_exists(pk) OR #expires < :now",
                ExpressionAttributeNames={"#expires": "expires"},
                ExpressionAttributeValues={":now": now},
            )
            break
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            if time.monotonic() >= deadline:
                raise RuntimeError(f"PDF processing is already in progress: {source}") from error
            time.sleep(2)
    try:
        yield
    finally:
        table().delete_item(
            Key={"pk": source, "sk": "LOCK"},
            ConditionExpression="#owner = :owner",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={":owner": owner},
        )


def mark_part(source, generation, part):
    table().put_item(Item={
        "pk": source, "sk": f"PART#{generation}#{part}",
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

import logging
import os
import shutil
import tempfile
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree

import boto3
import urllib3
from boto3.dynamodb.conditions import Key
from urllib3.util import Retry, Timeout


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
http = urllib3.PoolManager(
    retries=Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]),
    timeout=Timeout(connect=10, read=60),
)
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
WORK_DIR = Path(tempfile.gettempdir()) / "alexandria-download"
MAX_FEED_BYTES = 5 * 1024 * 1024


def get_response(url):
    response = http.request("GET", url, preload_content=False)
    if not 200 <= response.status < 300:
        response.close()
        response.release_conn()
        raise ValueError(f"HTTP {response.status} fetching {url}")
    return response


def newest_rss_date(url):
    response = get_response(url)
    try:
        data = response.read(MAX_FEED_BYTES + 1)
        if len(data) > MAX_FEED_BYTES:
            raise ValueError(f"RSS feed exceeds {MAX_FEED_BYTES} bytes: {url}")
    finally:
        response.close()
        response.release_conn()

    root = ElementTree.fromstring(data)
    dates = []
    for item in root.findall("./channel/item"):
        value = item.findtext("pubDate")
        if not value:
            raise ValueError(f"RSS item has no pubDate: {url}")
        date = parsedate_to_datetime(value.strip())
        if date.tzinfo is None:
            raise ValueError(f"RSS pubDate has no timezone: {value}")
        dates.append((date.astimezone(timezone.utc), value.strip()))
    if not dates:
        raise ValueError(f"RSS feed has no items: {url}")
    return max(dates)[1]


def download_pdf(url, path):
    response = get_response(url)
    try:
        with path.open("wb") as file:
            for chunk in response.stream(64 * 1024):
                file.write(chunk)
        with path.open("rb") as file:
            if b"%PDF-" not in file.read(1024):
                raise ValueError(f"Response is not a PDF: {url}")
    finally:
        response.close()
        response.release_conn()


def process_item(table, bucket, item):
    date = newest_rss_date(item["rss"])
    if date == item.get("lastupdate"):
        logger.info("No update for %s", item["sk"])
        return

    with tempfile.TemporaryDirectory(dir=WORK_DIR) as directory:
        path = Path(directory) / "download.pdf"
        download_pdf(item["link"], path)
        s3.upload_file(
            str(path), bucket, item["fname"],
            ExtraArgs={"ContentType": "application/pdf"},
        )
    table.update_item(
        Key={"pk": item["pk"], "sk": item["sk"]},
        UpdateExpression="SET lastupdate = :date",
        ConditionExpression="attribute_exists(pk) AND attribute_exists(sk)",
        ExpressionAttributeValues={":date": date},
    )
    logger.info("Downloaded %s", item["sk"])


def lambda_handler(event, context):
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    bucket = os.environ["BUCKET_NAME"]
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir()
    failures = []
    try:
        query = {"KeyConditionExpression": Key("pk").eq("PDF#") & Key("sk").begins_with("PDF#")}
        while True:
            result = table.query(**query)
            for item in result["Items"]:
                try:
                    process_item(table, bucket, item)
                except Exception:
                    logger.exception("Failed to process %s", item.get("sk", "<unknown>"))
                    failures.append(item.get("sk", "<unknown>"))
            if "LastEvaluatedKey" not in result:
                break
            query["ExclusiveStartKey"] = result["LastEvaluatedKey"]
    finally:
        shutil.rmtree(WORK_DIR)
    if failures:
        raise RuntimeError(f"Failed to process {len(failures)} PDF record(s): {', '.join(failures)}")
    return {"status": "ok"}

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import aws_cdk as cdk
import boto3
from aws_cdk.assertions import Match, Template

from alexandria.alexandria_download import AlexandriaDownloadStack


class DownloadStackTest(unittest.TestCase):
    def test_resources(self):
        template = Template.from_stack(AlexandriaDownloadStack(cdk.App(), "Download"))
        template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "download.lambda_handler",
            "Runtime": "python3.13",
            "Architectures": ["arm64"],
            "Timeout": 900,
            "MemorySize": 1024,
            "EphemeralStorage": {"Size": 1024},
            "ReservedConcurrentExecutions": 1,
            "Environment": {"Variables": Match.object_like({"TABLE_NAME": "pdf"})},
        })
        template.has_resource_properties("AWS::S3::Bucket", {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        })
        template.has_resource_properties("AWS::Events::Rule", {
            "ScheduleExpression": "cron(0 0 * * ? *)",
            "State": "ENABLED",
        })
        template.resource_count_is("AWS::Logs::LogGroup", 1)
        for action in [
            ["dynamodb:Query", "dynamodb:UpdateItem"],
            Match.array_with(["s3:PutObject"]),
            Match.array_with(["logs:PutLogEvents"]),
        ]:
            template.has_resource_properties("AWS::IAM::Policy", {
                "PolicyDocument": {"Statement": Match.array_with([
                    Match.object_like({"Action": action}),
                ])},
            })


spec = importlib.util.spec_from_file_location(
    "download_handler", Path(__file__).parents[1] / "download/download.py"
)
handler = importlib.util.module_from_spec(spec)
with patch.object(boto3, "resource"), patch.object(boto3, "client"):
    spec.loader.exec_module(handler)


def response(data, status=200):
    result = Mock()
    result.status = status
    result.read.return_value = data
    result.stream.return_value = [data]
    return result


class DownloadHandlerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work_dir = Path(self.temp.name) / "work"
        self.work_dir.mkdir()
        self.work_patch = patch.object(handler, "WORK_DIR", self.work_dir)
        self.work_patch.start()
        self.addCleanup(self.work_patch.stop)
        self.item = {
            "pk": "PDF#", "sk": "PDF#guide.pdf", "fname": "guide.pdf",
            "rss": "https://example.com/feed", "link": "https://example.com/guide.pdf",
            "lastupdate": "TBD",
        }

    def test_newest_rss_date_is_not_assumed_to_be_first(self):
        feed = b"""<rss><channel>
            <item><pubDate>Wed, 01 Jan 2025 00:00:00 GMT</pubDate></item>
            <item><pubDate>Tue, 31 Dec 2024 20:00:00 -0500</pubDate></item>
        </channel></rss>"""
        with patch.object(handler.http, "request", return_value=response(feed)):
            self.assertEqual(handler.newest_rss_date(self.item["rss"]),
                             "Tue, 31 Dec 2024 20:00:00 -0500")

    def test_upload_before_update_and_cleanup(self):
        table = Mock()
        s3 = Mock()
        order = Mock()
        order.attach_mock(s3, "s3")
        order.attach_mock(table, "table")
        pdf = response(b"%PDF-1.7\ncontent")
        with patch.object(handler, "newest_rss_date", return_value="new date"), \
             patch.object(handler.http, "request", return_value=pdf), \
             patch.object(handler, "s3", s3):
            handler.process_item(table, "bucket", self.item)
        self.assertEqual([entry[0] for entry in order.mock_calls],
                         ["s3.upload_file", "table.update_item"])
        self.assertEqual(s3.upload_file.call_args.args[1:], ("bucket", "guide.pdf"))
        self.assertEqual(table.update_item.call_args.kwargs["ExpressionAttributeValues"],
                         {":date": "new date"})
        self.assertEqual(list(self.work_dir.iterdir()), [])

    def test_unchanged_date_skips_download(self):
        table = Mock()
        with patch.object(handler, "newest_rss_date", return_value="TBD"), \
             patch.object(handler, "download_pdf") as download:
            handler.process_item(table, "bucket", self.item)
        download.assert_not_called()
        table.update_item.assert_not_called()

    def test_invalid_pdf_or_upload_failure_does_not_update(self):
        for data, upload_error in [(b"<html>not a PDF</html>", None),
                                   (b"%PDF-1.7\ncontent", OSError("upload failed"))]:
            with self.subTest(data=data):
                table = Mock()
                s3 = Mock()
                s3.upload_file.side_effect = upload_error
                with patch.object(handler, "newest_rss_date", return_value="new"), \
                     patch.object(handler.http, "request", return_value=response(data)), \
                     patch.object(handler, "s3", s3):
                    with self.assertRaises((ValueError, OSError)):
                        handler.process_item(table, "bucket", self.item)
                table.update_item.assert_not_called()
                self.assertEqual(list(self.work_dir.iterdir()), [])

    def test_handler_paginates_continues_after_failure_and_cleans_stale_files(self):
        (self.work_dir / "stale").write_text("stale")
        table = Mock()
        other = dict(self.item, sk="PDF#other.pdf")
        table.query.side_effect = [
            {"Items": [self.item], "LastEvaluatedKey": {"pk": "PDF#", "sk": "PDF#guide.pdf"}},
            {"Items": [other]},
        ]
        with patch.dict(os.environ, {"TABLE_NAME": "pdf", "BUCKET_NAME": "bucket"}), \
             patch.object(handler.dynamodb, "Table", return_value=table), \
             patch.object(handler, "process_item", side_effect=[ValueError("broken"), None]) as process:
            with self.assertRaisesRegex(RuntimeError, "1 PDF record"):
                handler.lambda_handler({}, None)
        self.assertEqual(process.call_count, 2)
        self.assertEqual(table.query.call_args_list[1].kwargs["ExclusiveStartKey"],
                         {"pk": "PDF#", "sk": "PDF#guide.pdf"})
        self.assertFalse(self.work_dir.exists())

    def test_feed_failures_are_reported(self):
        for result in [response(b"", status=500), response(b"<rss><channel/></rss>"),
                       response(b"x" * (handler.MAX_FEED_BYTES + 1))]:
            with self.subTest(status=result.status), \
                 patch.object(handler.http, "request", return_value=result):
                with self.assertRaises(ValueError):
                    handler.newest_rss_date(self.item["rss"])
                result.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()

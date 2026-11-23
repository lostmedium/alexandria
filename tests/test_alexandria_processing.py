import hashlib
import importlib.util
import io
import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote_plus

import boto3
import pymupdf
from aws_cdk import App
from aws_cdk.assertions import Match, Template

from alexandria.alexandria_download import AlexandriaDownloadStack
from alexandria.alexandria_processing import AlexandriaProcessingStack


spec = importlib.util.spec_from_file_location(
    "processing", Path(__file__).parents[1] / "processing/processing.py"
)
processing = importlib.util.module_from_spec(spec)
with patch.object(boto3, "client"):
    spec.loader.exec_module(processing)


def event(bucket, key, **obj):
    return {"Records": [{"body": json.dumps({"Records": [{
        "s3": {"bucket": {"name": bucket}, "object": {"key": quote_plus(key), **obj}}
    }]})}]}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.requests = []

    def get_object(self, **args):
        self.requests.append(args)
        item = self.objects[(args["Bucket"], args["Key"])]
        if "IfMatch" in args and args["IfMatch"] != item["etag"]:
            raise ValueError("ETag mismatch")
        return {"Body": io.BytesIO(item["body"]), "Metadata": item.get("metadata", {})}

    def put_object(self, Bucket, Key, Body, ContentType, Metadata=None):
        self.objects[(Bucket, Key)] = {
            "body": Body, "metadata": Metadata or {}, "etag": hashlib.md5(Body).hexdigest(),
        }

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.put_object(bucket, key, Path(path).read_bytes(), **ExtraArgs)


class ProcessingTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        work_patch = patch.object(processing, "WORK_DIR", Path(temporary.name) / "work")
        work_patch.start()
        self.addCleanup(work_patch.stop)
        self.s3 = FakeS3()
        self.s3_patch = patch.object(processing, "s3", self.s3)
        self.s3_patch.start()
        self.addCleanup(self.s3_patch.stop)

    def test_output_stages_use_one_filename_folder(self):
        source_key = "guides/My guide.pdf"
        etag = "current"
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        self.s3.objects[("downloads", source_key)] = {"body": data, "etag": etag}

        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(event("downloads", source_key, eTag=etag), None)
        part_keys = [key for bucket, key in self.s3.objects if bucket == "parts"]
        self.assertEqual(len(part_keys), 1)
        part_key = part_keys[0]
        self.assertEqual(part_key.split("/", 1)[0], "My guide")
        self.assertRegex(
            part_key,
            r"^My guide/source-[0-9a-f]{64}__revision-[0-9a-f]{64}__part-000001\.pdf$",
        )

        with patch.object(processing.pymupdf4llm, "to_markdown", return_value="# Title\n\nBody\n"), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            processing.convert_handler(event("parts", part_key), None)
        markdown_key = part_key[:-4] + ".md"
        self.assertIn(("markdown", markdown_key), self.s3.objects)

        with patch.dict(os.environ, {"DESTINATION_BUCKET": "chunks"}):
            processing.chunk_handler(event("markdown", markdown_key), None)
        chunk_key = markdown_key[:-3] + "-chunk-000001.md"
        self.assertIn(("chunks", chunk_key), self.s3.objects)
        self.assertEqual(chunk_key.split("/", 1)[0], "My guide")

    def test_pdf_events_produce_chunks_without_inference(self):
        with pymupdf.open() as pdf:
            for content in ("First page text.", "Second page text."):
                page = pdf.new_page()
                page.insert_text((72, 72), content)
            data = pdf.tobytes()
        key = "guides/My guide.pdf"
        self.s3.objects[("downloads", key)] = {"body": data, "etag": "first"}

        with patch.object(processing, "PAGE_LIMIT", 1), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(event("downloads", key, eTag="first"), None)
            self.s3.objects[("downloads", key)] = {"body": data, "etag": "second"}
            processing.split_handler(event("downloads", key, eTag="second"), None)
        parts = [key for bucket, key in self.s3.objects if bucket == "parts"]
        self.assertEqual(len(parts), 4)
        revisions = {
            hashlib.sha256(etag.encode()).hexdigest() for etag in ("first", "second")
        }
        source_id = hashlib.sha256(key.encode()).hexdigest()
        self.assertTrue(all(re.fullmatch(
            r"My guide/source-[0-9a-f]{64}__revision-[0-9a-f]{64}__part-\d{6}\.pdf",
            part,
        ) for part in parts))
        self.assertTrue(all(part.startswith(f"My guide/source-{source_id}__")
                            for part in parts))
        self.assertEqual(
            {re.search(r"__revision-([0-9a-f]{64})__", part).group(1) for part in parts},
            revisions,
        )
        self.assertEqual({part.split("/", 1)[0] for part in parts}, {"My guide"})
        self.assertEqual(self.s3.requests[0]["IfMatch"], "first")

        with patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            for part in parts:
                processing.convert_handler(event("parts", part), None)
        markdown = [key for bucket, key in self.s3.objects if bucket == "markdown"]
        self.assertEqual(len(markdown), 4)
        self.assertEqual({
            key.replace(".md", ".pdf")
            for key in markdown
        }, set(parts))
        self.assertTrue(all("__revision-" in key and "__part-" in key
                            for key in markdown))
        self.assertEqual({key.split("/", 1)[0] for key in markdown}, {"My guide"})

        with patch.dict(os.environ, {"DESTINATION_BUCKET": "chunks"}):
            for item in markdown:
                processing.chunk_handler(event("markdown", item), None)
        chunk_objects = {
            key: item for (bucket, key), item in self.s3.objects.items()
            if bucket == "chunks"
        }
        chunks = [item["body"].decode() for item in chunk_objects.values()]
        self.assertEqual(len(chunks), 4)
        self.assertTrue(all(re.fullmatch(
            r"My guide/source-[0-9a-f]{64}__revision-[0-9a-f]{64}"
            r"__part-\d{6}-chunk-\d{6}\.md", key
        ) for key in chunk_objects))
        self.assertEqual({key.split("/", 1)[0] for key in chunk_objects}, {"My guide"})
        self.assertEqual(
            set(chunk_objects),
            {key.replace(".md", "-chunk-000001.md")
             for key in markdown},
        )
        self.assertTrue(all("> Source: guides/My guide.pdf" in chunk for chunk in chunks))
        self.assertTrue(any("First page text" in chunk for chunk in chunks))
        self.assertTrue(any("Second page text" in chunk for chunk in chunks))

    def test_unversioned_event_uses_etag_and_failures_propagate(self):
        processing.WORK_DIR.mkdir()
        (processing.WORK_DIR / "stale").write_text("interrupted")
        self.s3.objects[("downloads", "a.pdf")] = {"body": b"wrong", "etag": "new"}
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            with self.assertRaisesRegex(ValueError, "ETag mismatch"):
                processing.split_handler(event("downloads", "a.pdf", eTag="old"), None)
            with self.assertRaisesRegex(ValueError, "Missing ETag"):
                processing.split_handler(event("downloads", "a.pdf"), None)
        self.assertEqual(self.s3.requests[0]["IfMatch"], "old")
        self.assertFalse(processing.WORK_DIR.exists())

    def test_ignores_s3_test_event_and_non_pdf(self):
        messages = {"Records": [{"body": json.dumps({"Event": "s3:TestEvent"})}]}
        messages["Records"].extend(event("downloads", "readme.txt")["Records"])
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(messages, None)
        self.assertFalse(self.s3.requests)

    def test_chunking_preserves_tables_fences_and_oversized_text(self):
        text = "# Topic\n\n" + ("Intro.\n\n" * 20) + "| A | B |\n| 1 | 2 |\n\n"
        text += "```python\n# not a heading\n\nprint('hello')\n```\n\n" + "x" * 220
        with patch.object(processing, "CHUNK_LIMIT", 100):
            chunks = processing.split_markdown(text)
        for chunk in chunks:
            if chunk.startswith("# Topic (continued)\n\n"):
                chunk = chunk.removeprefix("# Topic (continued)\n\n")
            self.assertLessEqual(len(chunk), 100)
        self.assertTrue(any("| A | B |\n| 1 | 2 |" in chunk for chunk in chunks))
        self.assertTrue(any("```python\n# not a heading\n\nprint('hello')\n```" in chunk
                            for chunk in chunks))
        self.assertEqual(sum(chunk.count("x") for chunk in chunks), 220)
        self.assertTrue(any("(continued)" in chunk for chunk in chunks))


class ProcessingStackTest(unittest.TestCase):
    def test_queues_notifications_and_terminal_chunk_bucket(self):
        app = App()
        download = AlexandriaDownloadStack(app, "Download")
        stack = AlexandriaProcessingStack(
            app, "Processing", download_bucket_name=download.bucket_name
        )
        template = Template.from_stack(stack)
        download_template = Template.from_stack(download)
        download_template.resource_count_is("AWS::SQS::Queue", 0)
        download_template.resource_count_is("Custom::S3BucketNotifications", 0)
        template.resource_count_is("AWS::SQS::Queue", 6)
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 3)
        template.has_resource_properties("AWS::SQS::Queue", {
            "VisibilityTimeout": 5400, "MessageRetentionPeriod": 1209600,
            "RedrivePolicy": Match.object_like({"maxReceiveCount": 5}),
        })
        for stack_template in (download_template, template):
            for resource in stack_template.find_resources("AWS::S3::Bucket").values():
                self.assertNotIn("VersioningConfiguration", resource.get("Properties", {}))
        for handler in ("split_handler", "convert_handler", "chunk_handler"):
            template.has_resource_properties("AWS::Lambda::Function", {
                "PackageType": "Image", "Timeout": 900, "MemorySize": 4096,
                "ImageConfig": {"Command": [f"processing.{handler}"]},
            })
        template.resource_count_is("Custom::S3BucketNotifications", 3)
        template.has_resource_properties("Custom::S3BucketNotifications", {
            "BucketName": {"Fn::ImportValue": Match.any_value()},
            "NotificationConfiguration": {
                "QueueConfigurations": Match.array_with([Match.object_like({
                    "Events": ["s3:ObjectCreated:*"],
                })]),
            },
        })
        assembly = app.synth()
        dependencies = assembly.get_stack_artifact(stack.artifact_id).dependencies
        self.assertIn(download.artifact_id, [dependency.id for dependency in dependencies])
        template.resource_count_is("AWS::Bedrock::ModelInvocationJob", 0)


if __name__ == "__main__":
    unittest.main()

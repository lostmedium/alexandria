import hashlib
import importlib.util
import json
import os
import re
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote_plus

import boto3
import pymupdf
from aws_cdk import App
from aws_cdk.assertions import Match, Template

from test_alexandria_support import FakeCoordination, FakeS3
from alexandria.alexandria_coordination import AlexandriaCoordinationStack
from alexandria.alexandria_download import AlexandriaDownloadStack
from alexandria.alexandria_processing import AlexandriaProcessingStack


spec = importlib.util.spec_from_file_location(
    "processing", Path(__file__).parents[1] / "processing/processing.py"
)
processing = importlib.util.module_from_spec(spec)
with patch.object(boto3, "client"):
    spec.loader.exec_module(processing)

stage_spec = importlib.util.spec_from_file_location(
    "staging_integration", Path(__file__).parents[1] / "staging/staging.py"
)
staging = importlib.util.module_from_spec(stage_spec)
with patch.object(boto3, "client"):
    stage_spec.loader.exec_module(staging)


def event(bucket, key, **obj):
    return {"Records": [{"body": json.dumps({"Records": [{
        "s3": {"bucket": {"name": bucket}, "object": {"key": quote_plus(key), **obj}}
    }]})}]}


class ProcessingTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.s3 = FakeS3()
        self.coordination = FakeCoordination()
        for attribute, value in (
            ("WORK_DIR", Path(temporary.name) / "work"),
            ("s3", self.s3), ("coordination", self.coordination), ("cleanup", self.cleanup),
        ):
            change = patch.object(processing, attribute, value)
            change.start()
            self.addCleanup(change.stop)
        env = patch.dict(os.environ, {"SOURCE_BUCKET": "downloads"})
        env.start()
        self.addCleanup(env.stop)

    def cleanup(self, key):
        prefix = (
            f"{key.rsplit('/', 1)[-1][:-4]}/"
            f"source-{self.coordination.source_id(key)}__"
        )
        marker = f"__completion__/{self.coordination.source_id(key)}/"
        for bucket, name in list(self.s3.objects):
            if bucket in ("parts", "markdown", "chunks") and (
                name.startswith(prefix) or (bucket == "chunks" and name.startswith(marker))
            ):
                del self.s3.objects[(bucket, name)]

    def put_pdf(self, key, data):
        self.s3.put_object("downloads", key, data, "application/pdf")
        return self.object_event("downloads", key)

    def object_event(self, bucket, key):
        item = self.s3.objects[(bucket, key)]
        return event(bucket, key, eTag=item["etag"], versionId=item["version"])

    def run_split(self, source_event):
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(source_event, None)

    def run_convert(self, key, text):
        with patch.object(processing.pymupdf4llm, "to_markdown", return_value=text), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            processing.convert_handler(self.object_event("parts", key), None)

    def run_chunk(self, key):
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "chunks"}):
            processing.chunk_handler(self.object_event("markdown", key), None)

    def test_completion_requires_all_parts_and_reprocessing_removes_old_chunks(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            pdf.new_page()
            data = pdf.tobytes()
        key = "guides/My guide.pdf"
        source_event = self.put_pdf(key, data)
        with patch.object(processing, "PAGE_LIMIT", 1):
            self.run_split(source_event)
        parts = sorted(name for bucket, name in self.s3.objects if bucket == "parts")
        self.assertEqual(len(parts), 2)
        self.assertTrue(all(re.fullmatch(
            r"My guide/source-[0-9a-f]{64}__revision-[0-9a-f]{64}"
            r"__generation-[0-9a-f]{64}__part-\d{6}\.pdf", part,
        ) for part in parts))
        for part in parts:
            self.run_convert(part, "# Title\n\nold\n")
        markdown = [part[:-4] + ".md" for part in parts]
        self.run_chunk(markdown[0])
        self.assertFalse(any(name.startswith("__completion__/") for _, name in self.s3.objects))
        self.run_chunk(markdown[1])
        markers = [name for bucket, name in self.s3.objects
                   if bucket == "chunks" and name.startswith("__completion__/")]
        self.assertEqual(len(markers), 1)
        before = self.s3.objects[("chunks", markers[0])]["version"]

        self.run_convert(parts[0], "# Title\n\nnew\n")
        self.run_chunk(markdown[0])
        chunk_keys = [name for bucket, name in self.s3.objects
                      if bucket == "chunks" and name.startswith(markdown[0][:-3] + "-chunk-")]
        self.assertEqual(len(chunk_keys), 1)
        chunk = self.s3.objects[("chunks", chunk_keys[0])]
        self.assertIn(b"new", chunk["body"])
        self.assertEqual(chunk["metadata"]["sha256"], hashlib.sha256(chunk["body"]).hexdigest())
        self.assertNotEqual(before, self.s3.objects[("chunks", markers[0])]["version"])

    def test_old_events_cannot_recreate_outputs_after_update(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        key = "guide.pdf"
        old_source = self.put_pdf(key, data)
        self.run_split(old_source)
        old_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        old_part_event = self.object_event("parts", old_part)
        self.run_convert(old_part, "# Old\n")
        old_markdown = old_part[:-4] + ".md"
        old_markdown_event = self.object_event("markdown", old_markdown)
        self.run_chunk(old_markdown)
        old_generation = self.coordination.state(self.coordination.source_id(key))["generation"]
        self.assertEqual(self.coordination.completed_parts(
            self.coordination.source_id(key), old_generation), 1)
        new_source = self.put_pdf(key, data + b"\n")
        self.run_split(new_source)
        self.assertEqual(self.coordination.completed_parts(
            self.coordination.source_id(key), old_generation), 0)
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(old_source, None)
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            processing.convert_handler(old_part_event, None)
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "chunks"}):
            processing.chunk_handler(old_markdown_event, None)
        self.assertNotIn(("parts", old_part), self.s3.objects)
        self.assertNotIn(("markdown", old_markdown), self.s3.objects)
        self.assertFalse(any(bucket == "chunks" for bucket, _ in self.s3.objects))

    def test_duplicate_pdf_event_does_not_remove_existing_parts(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        self.run_split(source_event)
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        version = self.s3.objects[("parts", part)]["version"]
        self.run_split(source_event)
        self.assertEqual(self.s3.objects[("parts", part)]["version"], version)

    def test_identical_pdf_bytes_in_new_version_start_new_generation(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        self.run_split(self.put_pdf("guide.pdf", data))
        old_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_split(self.put_pdf("guide.pdf", data))
        new_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.assertNotEqual(old_part, new_part)
        self.assertIn(f"__revision-{hashlib.sha256(data).hexdigest()}__", new_part)
        self.assertNotIn(("parts", old_part), self.s3.objects)

    def test_overlapping_old_conversion_is_cleaned_before_new_split(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        source_event = self.put_pdf("guide.pdf", data)
        self.run_split(source_event)
        old_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        part_event = self.object_event("parts", old_part)
        entered, release, split_finished = threading.Event(), threading.Event(), threading.Event()
        errors = []

        def convert():
            try:
                processing.convert_handler(part_event, None)
            except Exception as error:
                errors.append(error)

        def split(new_event):
            try:
                processing.split_handler(new_event, None)
            except Exception as error:
                errors.append(error)
            finally:
                split_finished.set()

        def extract(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test did not release conversion")
            return "# Old\n"

        with patch.object(processing.pymupdf4llm, "to_markdown", side_effect=extract), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            old_worker = threading.Thread(target=convert)
            old_worker.start()
            self.assertTrue(entered.wait(5))
            new_event = self.put_pdf("guide.pdf", data + b"\n")
            os.environ["DESTINATION_BUCKET"] = "parts"
            new_worker = threading.Thread(target=split, args=(new_event,))
            new_worker.start()
            try:
                self.assertFalse(split_finished.wait(0.2))
            finally:
                release.set()
                old_worker.join(5)
                new_worker.join(5)
            self.assertTrue(split_finished.is_set())
        self.assertEqual(errors, [])
        self.assertNotIn(("markdown", old_part[:-4] + ".md"), self.s3.objects)
        self.assertNotIn(("parts", old_part), self.s3.objects)

    def test_completion_events_publish_only_complete_current_revision(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        key = "guide.pdf"
        self.run_split(self.put_pdf(key, data))
        old_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(old_part, "# Old\n")
        self.run_chunk(old_part[:-4] + ".md")
        old_marker = next(name for bucket, name in self.s3.objects
                          if bucket == "chunks" and name.startswith("__completion__/"))
        old_event = self.object_event("chunks", old_marker)
        source_id = self.coordination.source_id(key)
        manifest_key = f"guide/source-{source_id}__manifest.json"
        with patch.object(staging, "s3", self.s3), \
             patch.object(staging, "coordination", self.coordination), \
             patch.dict(os.environ, {"CHUNKS_BUCKET": "chunks", "STAGING_BUCKET": "staging"}):
            staging.lambda_handler(old_event, None)
            old_manifest = json.loads(self.s3.objects[("staging", manifest_key)]["body"])
            self.run_split(self.put_pdf(key, data + b"\n"))
            self.assertEqual(
                json.loads(self.s3.objects[("staging", manifest_key)]["body"]), old_manifest,
            )
            new_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
            self.run_convert(new_part, "# New\n")
            self.run_chunk(new_part[:-4] + ".md")
            marker = next(name for bucket, name in self.s3.objects
                          if bucket == "chunks" and name.startswith("__completion__/"))
            staging.lambda_handler(self.object_event("chunks", marker), None)
            new_manifest = json.loads(self.s3.objects[("staging", manifest_key)]["body"])
            self.assertNotEqual(new_manifest["generation"], old_manifest["generation"])
            staging.lambda_handler(old_event, None)
            self.assertEqual(
                json.loads(self.s3.objects[("staging", manifest_key)]["body"]), new_manifest,
            )
            for entry in old_manifest["chunks"]:
                self.assertIn(("staging", entry["key"], entry["version_id"]), self.s3.versions)

    def test_missing_etag_and_non_pdf_events(self):
        messages = {"Records": [{"body": json.dumps({"Event": "s3:TestEvent"})}]}
        messages["Records"].extend(event("downloads", "readme.txt")["Records"])
        with patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
            processing.split_handler(messages, None)
            with self.assertRaisesRegex(ValueError, "Missing ETag"):
                processing.split_handler(event("downloads", "a.pdf"), None)

    def test_split_rejects_mismatched_etag(self):
        source_event = self.put_pdf("guide.pdf", b"pdf")
        obj = json.loads(source_event["Records"][0]["body"])["Records"][0]["s3"]["object"]
        obj["eTag"] = "wrong"
        source_event["Records"][0]["body"] = json.dumps({"Records": [{"s3": {
            "bucket": {"name": "downloads"}, "object": obj,
        }}]})
        with self.assertRaisesRegex(ValueError, "ETag mismatch"):
            self.run_split(source_event)

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
    def test_queues_notifications_and_buckets(self):
        app = App()
        coordination = AlexandriaCoordinationStack(app, "Coordination")
        download = AlexandriaDownloadStack(
            app, "Download", coordination_table=coordination.table,
            coordination_layer=coordination.layer,
        )
        stack = AlexandriaProcessingStack(
            app, "Processing", download_bucket_name=download.bucket_name,
            cleanup_function_name=download.cleanup_function_name,
            coordination_table=coordination.table,
        )
        template = Template.from_stack(stack)
        download_template = Template.from_stack(download)
        download_template.resource_count_is("AWS::SQS::Queue", 0)
        template.resource_count_is("AWS::SQS::Queue", 6)
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 3)
        template.has_resource_properties("AWS::SQS::Queue", {
            "VisibilityTimeout": 5400, "MessageRetentionPeriod": 1209600,
            "RedrivePolicy": Match.object_like({"maxReceiveCount": 100}),
        })
        for stack_template in (download_template, template):
            for resource in stack_template.find_resources("AWS::S3::Bucket").values():
                props = resource["Properties"]
                self.assertEqual(props["VersioningConfiguration"], {"Status": "Enabled"})
                self.assertEqual(props["LifecycleConfiguration"]["Rules"][0], {
                    "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
                })
        for handler in ("split_handler", "convert_handler", "chunk_handler"):
            template.has_resource_properties("AWS::Lambda::Function", {
                "PackageType": "Image", "Timeout": 900, "MemorySize": 4096,
                "ImageConfig": {"Command": [f"processing.{handler}"]},
            })
        template.resource_count_is("Custom::S3BucketNotifications", 3)
        assembly = app.synth()
        dependencies = assembly.get_stack_artifact(stack.artifact_id).dependencies
        self.assertIn(download.artifact_id, [dependency.id for dependency in dependencies])


if __name__ == "__main__":
    unittest.main()

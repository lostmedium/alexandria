import hashlib
import importlib.util
import json
import os
import re
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch
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
    return {"Records": [{"messageId": "message", "receiptHandle": "receipt",
                        "body": json.dumps({"Records": [{
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
        env = patch.dict(os.environ, {
            "SOURCE_BUCKET": "downloads", "QUEUE_URL": "queue-url", "CHUNKS_BUCKET": "chunks",
        })
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

        original_part = self.s3.objects[("parts", parts[0])]
        self.s3.put_object(
            "parts", parts[0], original_part["body"], "application/pdf", original_part["metadata"],
        )
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

    def test_parts_convert_and_chunk_before_split_finishes(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        unlink = Path.unlink
        processed = []
        source = self.coordination.source_id("guide.pdf")

        def process_part(path, *args, **kwargs):
            if path.name == "part.pdf":
                parts = sorted(name for bucket, name in self.s3.objects if bucket == "parts")
                part = parts[-1]
                state = self.coordination.state(source)
                self.assertEqual(state["status"], "splitting")
                self.assertEqual(state["expected"], 2)
                self.assertEqual(len(parts), len(processed) + 1)
                self.run_convert(part, "# Overlap\n")
                self.run_chunk(part[:-4] + ".md")
                processed.append(part)
                self.assertEqual(
                    self.coordination.completed_parts(source, state["generation"]), len(processed),
                )
                self.assertFalse(any(name.startswith("__completion__/")
                                     for _, name in self.s3.objects))
                with patch.object(processing, "sqs") as sqs:
                    result = processing.split_handler(source_event, None)
                self.assertEqual(result, {
                    "batchItemFailures": [{"itemIdentifier": "message"}],
                })
                sqs.change_message_visibility.assert_called_once()
            return unlink(path, *args, **kwargs)

        with patch.object(processing, "PAGE_LIMIT", 1), \
             patch.object(Path, "unlink", process_part):
            self.run_split(source_event)
        self.assertEqual(len(processed), 2)
        self.assertEqual(self.coordination.state(source)["status"], "ready")
        markers = [name for bucket, name in self.s3.objects
                   if bucket == "chunks" and name.startswith("__completion__/")]
        self.assertEqual(len(markers), 1)
        with patch.object(staging, "s3", self.s3), \
             patch.object(staging, "coordination", self.coordination), \
             patch.dict(os.environ, {"STAGING_BUCKET": "staging"}):
            staging.lambda_handler(self.object_event("chunks", markers[0]), None)
        manifest = json.loads(self.s3.objects[
            ("staging", f"guide/source-{source}__manifest.json")
        ]["body"])
        self.assertEqual(len(manifest["chunks"]), 2)

    def test_split_waits_for_chunk_publication_without_restarting(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        source = self.coordination.source_id("guide.pdf")
        original_locked = self.coordination.locked
        acquisitions = 0
        started = threading.Event()
        holder = None

        def hold_source():
            with original_locked(source):
                started.set()
                time.sleep(2.5)

        @contextmanager
        def contend(key, *, wait_seconds=30):
            nonlocal acquisitions, holder
            if key == source:
                acquisitions += 1
                if acquisitions == 3:
                    holder = threading.Thread(target=hold_source)
                    holder.start()
                    if not started.wait(5):
                        raise TimeoutError("test lock holder did not start")
            with original_locked(key, wait_seconds=wait_seconds):
                yield

        try:
            with patch.object(self.coordination, "locked", side_effect=contend), \
                 patch.object(processing, "sqs") as sqs, \
                 patch.dict(os.environ, {"DESTINATION_BUCKET": "parts"}):
                result = processing.split_handler(source_event, None)
            self.assertEqual(result, {"batchItemFailures": []})
            sqs.change_message_visibility.assert_not_called()
            self.assertEqual(self.coordination.state(source)["status"], "ready")
            self.assertEqual(sum(bucket == "parts" for bucket, _ in self.s3.objects), 1)
        finally:
            if holder is not None:
                holder.join(5)

    def test_failed_split_retry_invalidates_inflight_conversion_of_same_pdf_version(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        upload = self.s3.upload_file

        def fail_second(path, bucket, key, ExtraArgs):
            if key.endswith("000002.pdf"):
                raise RuntimeError("split interrupted")
            return upload(path, bucket, key, ExtraArgs)

        with patch.object(processing, "PAGE_LIMIT", 1), \
             patch.object(self.s3, "upload_file", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "split interrupted"):
                self.run_split(source_event)
        old_part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(old_part, "# Partial\n")
        self.run_chunk(old_part[:-4] + ".md")
        self.assertFalse(any(name.startswith("__completion__/") for _, name in self.s3.objects))
        old_event = self.object_event("parts", old_part)
        source = self.coordination.source_id("guide.pdf")
        old_generation = self.coordination.state(source)["generation"]
        item = self.s3.objects[("parts", old_part)]
        self.s3.put_object("parts", old_part, item["body"], "application/pdf", item["metadata"])

        def retry_split(*args, **kwargs):
            self.run_split(source_event)
            return "# Superseded\n"

        with patch.object(processing.pymupdf4llm, "to_markdown", side_effect=retry_split), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            processing.convert_handler(self.object_event("parts", old_part), None)
            processing.convert_handler(old_event, None)
        self.assertNotEqual(self.coordination.state(source)["generation"], old_generation)
        self.assertEqual(self.coordination.completed_parts(source, old_generation), 0)
        self.assertNotIn(("parts", old_part), self.s3.objects)
        self.assertFalse(any(bucket in ("markdown", "chunks") for bucket, _ in self.s3.objects))

    def test_replacement_between_part_uploads_cannot_finish_old_split(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            pdf.new_page()
            data = pdf.tobytes()
        old_event = self.put_pdf("guide.pdf", data)
        unlink = Path.unlink

        def replace(path, *args, **kwargs):
            if path.name == "part.pdf":
                self.put_pdf("guide.pdf", data + b"\n")
            return unlink(path, *args, **kwargs)

        with patch.object(processing, "PAGE_LIMIT", 1), patch.object(Path, "unlink", replace):
            self.run_split(old_event)
        source = self.coordination.source_id("guide.pdf")
        self.assertEqual(self.coordination.state(source)["status"], "splitting")
        self.assertEqual(sum(bucket == "parts" for bucket, _ in self.s3.objects), 1)
        self.assertFalse(any(name.startswith("__completion__/") for _, name in self.s3.objects))

    def test_split_retry_publishes_completion_after_marker_upload_failure(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        unlink = Path.unlink
        put_object = self.s3.put_object

        def finish_part(path, *args, **kwargs):
            if path.name == "part.pdf":
                part = next(name for bucket, name in self.s3.objects if bucket == "parts")
                self.run_convert(part, "# Complete\n")
                self.run_chunk(part[:-4] + ".md")
            return unlink(path, *args, **kwargs)

        def fail_marker(*args, **kwargs):
            if kwargs.get("Key", "").startswith("__completion__/"):
                raise RuntimeError("marker upload failed")
            return put_object(*args, **kwargs)

        with patch.object(Path, "unlink", finish_part), \
             patch.object(self.s3, "put_object", side_effect=fail_marker):
            with self.assertRaisesRegex(RuntimeError, "marker upload failed"):
                self.run_split(source_event)
        source = self.coordination.source_id("guide.pdf")
        generation = self.coordination.state(source)["generation"]
        with patch.object(self.s3, "upload_file") as upload:
            self.run_split(source_event)
        upload.assert_not_called()
        self.assertIn(("chunks", f"__completion__/{source}/{generation}.json"), self.s3.objects)

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

    def test_new_split_does_not_wait_for_old_conversion_or_publish_stale_markdown(self):
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
                self.assertTrue(split_finished.wait(5))
            finally:
                release.set()
                old_worker.join(5)
                new_worker.join(5)
            self.assertTrue(split_finished.is_set())
        self.assertEqual(errors, [])
        self.assertNotIn(("markdown", old_part[:-4] + ".md"), self.s3.objects)
        self.assertNotIn(("parts", old_part), self.s3.objects)

    def test_different_parts_convert_in_parallel(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            pdf.new_page()
            source_event = self.put_pdf("guide.pdf", pdf.tobytes())
        with patch.object(processing, "PAGE_LIMIT", 1):
            self.run_split(source_event)
        parts = sorted(name for bucket, name in self.s3.objects if bucket == "parts")
        barrier = threading.Barrier(2, timeout=5)
        errors = []

        def extract(*args, **kwargs):
            barrier.wait()
            return "# Parallel\n"

        def convert(key):
            try:
                processing.convert_handler(self.object_event("parts", key), None)
            except Exception as error:
                errors.append(error)

        with patch.object(processing.pymupdf4llm, "to_markdown", side_effect=extract), \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            workers = [threading.Thread(target=convert, args=(key,)) for key in parts]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(10)
        self.assertEqual(errors, [])
        self.assertTrue(all(("markdown", key[:-4] + ".md") in self.s3.objects for key in parts))

    def test_duplicate_conversion_skips_extraction_and_preserves_markdown_version(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            self.run_split(self.put_pdf("guide.pdf", pdf.tobytes()))
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(part, "# Once\n")
        before = self.s3.objects[("markdown", part[:-4] + ".md")]["version"]
        with patch.object(processing.pymupdf4llm, "to_markdown") as extract, \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            result = processing.convert_handler(self.object_event("parts", part), None)
        extract.assert_not_called()
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(self.s3.objects[("markdown", part[:-4] + ".md")]["version"], before)

    def test_busy_duplicate_and_not_ready_parts_retry_after_thirty_seconds(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            self.run_split(self.put_pdf("guide.pdf", pdf.tobytes()))
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        source, generation, number = processing.source_for_part(part)
        state = self.coordination.state(source)
        sqs = Mock()
        with patch.object(processing, "sqs", sqs), \
             patch.object(processing.pymupdf4llm, "to_markdown") as extract, \
             patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
            with self.coordination.locked(f"{source}:convert:{generation}:{number}"):
                result = processing.convert_handler(self.object_event("parts", part), None)
            self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "message"}]})
            state["status"] = "preparing"
            result = processing.convert_handler(self.object_event("parts", part), None)
            self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "message"}]})
        extract.assert_not_called()
        self.assertEqual(sqs.change_message_visibility.call_count, 2)
        sqs.change_message_visibility.assert_called_with(
            QueueUrl="queue-url", ReceiptHandle="receipt", VisibilityTimeout=30,
        )
        self.assertFalse(any(bucket == "markdown" for bucket, _ in self.s3.objects))

    def test_real_native_conversion_of_smaller_parts_preserves_text_and_headings(self):
        with pymupdf.open() as pdf:
            for number in range(51):
                page = pdf.new_page()
                page.insert_text((72, 72), "Native guide", fontsize=20)
                page.insert_text((72, 110), f"Searchable page {number + 1}", fontsize=10)
                if number == 0:
                    for x in (72, 172, 272):
                        page.draw_line((x, 150), (x, 210))
                    for y in (150, 180, 210):
                        page.draw_line((72, y), (272, y))
                    for x, y, value in (
                        (82, 170, "Field"), (182, 170, "Value"),
                        (82, 200, "Key"), (182, 200, "Example"),
                    ):
                        page.insert_text((x, y), value, fontsize=10)
            self.run_split(self.put_pdf("guide.pdf", pdf.tobytes()))
        parts = sorted(name for bucket, name in self.s3.objects if bucket == "parts")
        self.assertEqual(len(parts), 2)
        for key, expected_pages in zip(parts, (50, 1)):
            with pymupdf.open(stream=self.s3.objects[("parts", key)]["body"]) as pdf:
                self.assertEqual(pdf.page_count, expected_pages)
            with patch.object(
                processing.pymupdf4llm, "_layout_to_markdown",
                side_effect=AssertionError("ML layout must not run"),
            ), patch.dict(os.environ, {"DESTINATION_BUCKET": "markdown"}):
                processing.convert_handler(self.object_event("parts", key), None)
            self.run_chunk(key[:-4] + ".md")
        text = "".join(
            self.s3.objects[("markdown", key[:-4] + ".md")]["body"].decode() for key in parts
        )
        self.assertIn("# Native guide", text)
        self.assertRegex(text, r"\|\s*Field\s*\|\s*Value\s*\|")
        self.assertRegex(text, r"\|\s*Key\s*\|\s*Example\s*\|")
        self.assertTrue(all(f"Searchable page {number + 1}" in text for number in range(51)))
        source = self.coordination.source_id("guide.pdf")
        generation = self.coordination.state(source)["generation"]
        self.assertEqual(self.coordination.completed_parts(source, generation), 2)

    def test_failed_parallel_upload_does_not_mark_part_complete(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            self.run_split(self.put_pdf("guide.pdf", pdf.tobytes()))
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(part, "# Guide\n\n" + "x" * 40000)
        with patch.object(self.s3, "put_object", side_effect=RuntimeError("upload failed")):
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                self.run_chunk(part[:-4] + ".md")
        source = self.coordination.source_id("guide.pdf")
        generation = self.coordination.state(source)["generation"]
        self.assertEqual(self.coordination.completed_parts(source, generation), 0)

    def test_duplicate_chunk_event_does_not_reupload_unchanged_chunks(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            self.run_split(self.put_pdf("guide.pdf", pdf.tobytes()))
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(part, "# Guide\n")
        markdown = part[:-4] + ".md"
        self.run_chunk(markdown)
        chunk = next(key for bucket, key in self.s3.objects
                     if bucket == "chunks" and "-chunk-" in key)
        version = self.s3.objects[("chunks", chunk)]["version"]
        self.run_chunk(markdown)
        self.assertEqual(self.s3.objects[("chunks", chunk)]["version"], version)

    def test_replacement_during_chunk_preparation_discards_old_results(self):
        with pymupdf.open() as pdf:
            pdf.new_page()
            data = pdf.tobytes()
        self.run_split(self.put_pdf("guide.pdf", data))
        part = next(name for bucket, name in self.s3.objects if bucket == "parts")
        self.run_convert(part, "# Old\n")
        split_markdown = processing.split_markdown

        def replace(text):
            self.run_split(self.put_pdf("guide.pdf", data + b"\n"))
            return split_markdown(text)

        with patch.object(processing, "split_markdown", side_effect=replace):
            self.run_chunk(part[:-4] + ".md")
        self.assertFalse(any(bucket == "chunks" for bucket, _ in self.s3.objects))
        self.assertNotIn(("markdown", part[:-4] + ".md"), self.s3.objects)

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
        for resource in template.find_resources("AWS::Lambda::EventSourceMapping").values():
            self.assertEqual(resource["Properties"]["FunctionResponseTypes"],
                             ["ReportBatchItemFailures"])
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
                "Environment": {"Variables": Match.object_like({"QUEUE_URL": Match.any_value()})},
            })
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {"Statement": Match.array_with([
                Match.object_like({"Action": Match.array_with(["sqs:ChangeMessageVisibility"])}),
            ])},
        })
        split_function = next(
            resource for resource in template.find_resources("AWS::Lambda::Function").values()
            if resource["Properties"].get("ImageConfig", {}).get("Command")
            == ["processing.split_handler"]
        )
        split_role = split_function["Properties"]["Role"]["Fn::GetAtt"][0]
        chunks_id = next(
            logical_id for logical_id in template.find_resources("AWS::S3::Bucket")
            if logical_id.startswith("Chunks")
        )
        template.has_resource_properties("AWS::IAM::Policy", {
            "Roles": [{"Ref": split_role}],
            "PolicyDocument": {"Statement": Match.array_with([
                Match.object_like({
                    "Action": Match.array_with(["s3:PutObject"]),
                    "Resource": {
                        "Fn::Join": ["", [{"Fn::GetAtt": [chunks_id, "Arn"]}, "/*"]],
                    },
                }),
            ])},
        })
        template.resource_count_is("Custom::S3BucketNotifications", 3)
        assembly = app.synth()
        dependencies = assembly.get_stack_artifact(stack.artifact_id).dependencies
        self.assertIn(download.artifact_id, [dependency.id for dependency in dependencies])


if __name__ == "__main__":
    unittest.main()

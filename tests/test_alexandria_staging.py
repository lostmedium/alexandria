import hashlib
import importlib.util
import json
import os
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote_plus

import boto3
from aws_cdk import App
from aws_cdk.assertions import Match, Template

from test_alexandria_support import FakeCoordination, FakeS3
from alexandria.alexandria_coordination import AlexandriaCoordinationStack
from alexandria.alexandria_processing import AlexandriaProcessingStack
from alexandria.alexandria_staging import AlexandriaStagingStack


spec = importlib.util.spec_from_file_location(
    "staging", Path(__file__).parents[1] / "staging/staging.py"
)
staging = importlib.util.module_from_spec(spec)
with patch.object(boto3, "client"):
    spec.loader.exec_module(staging)


class StagingTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.coordination = FakeCoordination()
        self.key = "guides/a.pdf"
        self.source = self.coordination.source_id(self.key)
        version = self.s3.put_object("downloads", self.key, b"pdf", "application/pdf")["VersionId"]
        self.generation = self.coordination.generation_id(version)
        self.revision = hashlib.sha256(b"pdf").hexdigest()
        self.prefix = f"a/source-{self.source}__"
        self.revision_prefix = (
            f"{self.prefix}revision-{self.revision}__generation-{self.generation}__"
        )
        self.manifest_key = self.prefix + "manifest.json"
        self.coordination.put_state(
            self.source, status="ready", source_key=self.key, version=version,
            generation=self.generation, revision=self.revision, expected=2,
        )
        for part in ("000001", "000002"):
            self.coordination.mark_part(self.source, self.generation, part, 1)
            body = f"chunk {part}".encode()
            digest = hashlib.sha256(body).hexdigest()
            key = f"{self.revision_prefix}part-{part}-chunk-000001-{digest}.md"
            self.s3.put_object("chunks", key, body, "text/markdown", {"sha256": digest})
        self.marker = f"__completion__/{self.source}/{self.generation}.json"
        self.s3.put_object("chunks", self.marker, b"{}", "application/json")
        for attribute, value in (("s3", self.s3), ("coordination", self.coordination)):
            change = patch.object(staging, attribute, value)
            change.start()
            self.addCleanup(change.stop)
        environment = patch.dict(os.environ, {
            "CHUNKS_BUCKET": "chunks", "STAGING_BUCKET": "staging", "SOURCE_BUCKET": "downloads",
        })
        environment.start()
        self.addCleanup(environment.stop)

    def completion_event(self, version=None):
        version = version or self.s3.objects[("chunks", self.marker)]["version"]
        return {"Records": [{"body": json.dumps({"Records": [{
            "s3": {"object": {"key": quote_plus(self.marker), "versionId": version}}
        }]})}]}

    def test_completion_publishes_manifest_and_removes_only_old_pdf_chunks(self):
        old_key = self.prefix + "old.md"
        self.s3.put_object("staging", old_key, b"old", "text/markdown")
        old_version = self.s3.objects[("staging", old_key)]["version"]
        self.s3.put_object("staging", "other/untouched.md", b"other", "text/markdown")
        staging.lambda_handler(self.completion_event(), None)
        manifest = json.loads(self.s3.objects[("staging", self.manifest_key)]["body"])
        self.assertEqual(manifest["source_key"], self.key)
        self.assertEqual(manifest["generation"], self.generation)
        self.assertEqual(len(manifest["chunks"]), 2)
        for chunk in manifest["chunks"]:
            item = self.s3.objects[("staging", chunk["key"])]
            self.assertEqual(chunk["version_id"], item["version"])
            self.assertEqual(chunk["sha256"], hashlib.sha256(item["body"]).hexdigest())
        self.assertNotIn(("staging", old_key), self.s3.objects)
        self.assertIn(("staging", old_key, old_version), self.s3.versions)
        self.assertIn(("staging", "other/untouched.md"), self.s3.objects)
        staging.lambda_handler({"source_key": self.key}, None)
        self.assertEqual(len(self.s3.copies), 2)

    def test_copy_failure_preserves_previous_manifest_and_chunks(self):
        old_key = self.prefix + "old.md"
        self.s3.put_object("staging", old_key, b"old", "text/markdown")
        self.s3.put_object("staging", self.manifest_key, b'{"old": true}', "application/json")
        copy = self.s3.copy_object

        def partial_copy(**args):
            if "__part-000002-" in args["Key"]:
                raise RuntimeError("copy failed")
            return copy(**args)

        with patch.object(self.s3, "copy_object", side_effect=partial_copy), \
             self.assertRaisesRegex(RuntimeError, "copy failed"):
            staging.lambda_handler(self.completion_event(), None)
        self.assertEqual(self.s3.objects[("staging", self.manifest_key)]["body"], b'{"old": true}')
        self.assertIn(("staging", old_key), self.s3.objects)
        self.assertEqual(len(self.s3.copies), 1)

    def test_incomplete_or_stale_completion_does_not_publish(self):
        self.coordination.clear_part(self.source, self.generation, "000002")
        staging.lambda_handler(self.completion_event(), None)
        self.assertNotIn(("staging", self.manifest_key), self.s3.objects)
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            staging.lambda_handler({"source_key": self.key}, None)
        self.coordination.mark_part(self.source, self.generation, "000002", 1)
        old_event = self.completion_event()
        self.s3.put_object("chunks", self.marker, b"new", "application/json")
        staging.lambda_handler(old_event, None)
        self.assertNotIn(("staging", self.manifest_key), self.s3.objects)
        self.s3.put_object("downloads", self.key, b"new pdf", "application/pdf")
        staging.lambda_handler(self.completion_event(), None)
        self.assertNotIn(("staging", self.manifest_key), self.s3.objects)

    def test_missing_chunk_does_not_replace_previous_manifest(self):
        self.s3.put_object("staging", self.manifest_key, b'{"old": true}', "application/json")
        missing = next(key for bucket, key in self.s3.objects
                       if bucket == "chunks" and "__part-000002-" in key)
        del self.s3.objects[("chunks", missing)]
        with self.assertRaisesRegex(ValueError, "Missing or unexpected chunks"):
            staging.lambda_handler(self.completion_event(), None)
        self.assertEqual(self.s3.objects[("staging", self.manifest_key)]["body"], b'{"old": true}')
        self.assertEqual(self.s3.copies, [])

    def test_accepts_dynamodb_decimal_chunk_counts(self):
        for part in ("000001", "000002"):
            self.coordination.mark_part(self.source, self.generation, part, Decimal(1))
        staging.lambda_handler(self.completion_event(), None)
        self.assertIn(("staging", self.manifest_key), self.s3.objects)

    def test_missing_one_of_multiple_chunks_in_a_part_fails(self):
        self.coordination.mark_part(self.source, self.generation, "000001", 2)
        with self.assertRaisesRegex(ValueError, "Missing or unexpected chunks"):
            staging.lambda_handler(self.completion_event(), None)
        self.assertNotIn(("staging", self.manifest_key), self.s3.objects)


class StagingStackTest(unittest.TestCase):
    def test_staging_resources_and_processing_dependency(self):
        app = App()
        coordination = AlexandriaCoordinationStack(app, "Coordination")
        processing = AlexandriaProcessingStack(
            app, "Processing", download_bucket_name="downloads",
            cleanup_function_name="cleanup", coordination_table=coordination.table,
        )
        stack = AlexandriaStagingStack(
            app, "Staging", chunks_bucket_name=processing.chunks_bucket_name,
            source_bucket_name="downloads", coordination_table=coordination.table,
            coordination_layer=coordination.layer,
        )
        template = Template.from_stack(stack)
        template.has_resource_properties("AWS::S3::Bucket", {
            "VersioningConfiguration": {"Status": "Enabled"},
            "LifecycleConfiguration": {"Rules": [{
                "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
            }]},
        })
        template.resource_count_is("AWS::Events::Rule", 0)
        template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "staging.lambda_handler", "Timeout": 900,
        })
        template.has_resource_properties("AWS::SQS::Queue", {
            "VisibilityTimeout": 5400,
            "RedrivePolicy": Match.object_like({"maxReceiveCount": 100}),
        })
        template.has_resource_properties("Custom::S3BucketNotifications", {
            "NotificationConfiguration": {"QueueConfigurations": Match.array_with([
                Match.object_like({
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [
                        {"Name": "suffix", "Value": ".json"},
                        {"Name": "prefix", "Value": "__completion__/"},
                    ]}},
                }),
            ])},
        })
        assembly = app.synth()
        dependencies = assembly.get_stack_artifact(stack.artifact_id).dependencies
        self.assertIn(processing.artifact_id, [dependency.id for dependency in dependencies])

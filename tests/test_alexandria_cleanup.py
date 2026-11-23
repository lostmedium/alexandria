import hashlib
import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import boto3
from aws_cdk import App
from aws_cdk.assertions import Match, Template

from alexandria.alexandria_cleanup import AlexandriaCleanupStack
from alexandria.alexandria_coordination import AlexandriaCoordinationStack
from alexandria.alexandria_processing import AlexandriaProcessingStack


spec = importlib.util.spec_from_file_location(
    "cleanup", Path(__file__).parents[1] / "cleanup" / "cleanup.py"
)
cleanup = importlib.util.module_from_spec(spec)
with patch.object(boto3, "client"):
    spec.loader.exec_module(cleanup)


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.delete_error = False

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        keys = sorted(key for bucket, key in self.objects if bucket == Bucket
                      and key.startswith(Prefix))
        return iter({"Contents": [{"Key": key} for key in keys[i:i + 2]]}
                    for i in range(0, len(keys), 2))

    def delete_objects(self, Bucket, Delete):
        if self.delete_error:
            return {"Errors": [{"Code": "AccessDenied"}]}
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)
        return {}


class CleanupTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()

    def test_cleanup_deletes_only_target_source_across_all_pages(self):
        key = "guides/a.pdf"
        prefix = f"a/source-{hashlib.sha256(key.encode()).hexdigest()}__"
        for bucket in ("parts", "markdown", "chunks"):
            for index in range(5):
                self.s3.objects[(bucket, prefix + str(index))] = {}
            self.s3.objects[(bucket, "a/source-other__1")] = {}
        marker = f"__completion__/{hashlib.sha256(key.encode()).hexdigest()}/generation.json"
        self.s3.objects[("chunks", marker)] = {}
        self.s3.objects[("chunks", "__completion__/other/generation.json")] = {}
        with patch.object(cleanup, "s3", self.s3), patch.dict(os.environ, {
            "PARTS_BUCKET": "parts", "MARKDOWN_BUCKET": "markdown", "CHUNKS_BUCKET": "chunks",
        }):
            cleanup.lambda_handler({"key": key}, None)
        self.assertEqual(len(self.s3.objects), 4)
        self.assertNotIn(("chunks", marker), self.s3.objects)
        self.assertIn(("chunks", "__completion__/other/generation.json"), self.s3.objects)

    def test_cleanup_reports_delete_errors(self):
        key = "a.pdf"
        prefix = f"a/source-{hashlib.sha256(key.encode()).hexdigest()}__"
        self.s3.objects[("parts", prefix + "1")] = {}
        self.s3.delete_error = True
        with patch.object(cleanup, "s3", self.s3), patch.dict(os.environ, {
            "PARTS_BUCKET": "parts", "MARKDOWN_BUCKET": "markdown", "CHUNKS_BUCKET": "chunks",
        }), self.assertRaisesRegex(RuntimeError, "AccessDenied"):
            cleanup.lambda_handler({"key": key}, None)


class CleanupStackTest(unittest.TestCase):
    def test_cleanup_resources_and_processing_dependency(self):
        app = App()
        coordination = AlexandriaCoordinationStack(app, "Coordination")
        processing = AlexandriaProcessingStack(
            app, "Processing", download_bucket_name="downloads",
            cleanup_function_name="Download-PdfCleanup", coordination_table=coordination.table,
        )
        stack = AlexandriaCleanupStack(
            app, "Cleanup", cleanup_function_name="Download-PdfCleanup",
            parts_bucket_name=processing.parts_bucket_name,
            markdown_bucket_name=processing.markdown_bucket_name,
            chunks_bucket_name=processing.chunks_bucket_name,
        )
        template = Template.from_stack(stack)
        template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "cleanup.lambda_handler", "FunctionName": "Download-PdfCleanup",
            "Environment": {"Variables": {
                "PARTS_BUCKET": {"Fn::ImportValue": Match.any_value()},
                "MARKDOWN_BUCKET": {"Fn::ImportValue": Match.any_value()},
                "CHUNKS_BUCKET": {"Fn::ImportValue": Match.any_value()},
            }},
        })
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {"Statement": Match.array_with([
                Match.object_like({"Action": "s3:ListBucket"}),
                Match.object_like({"Action": "s3:DeleteObject"}),
            ])},
        })
        assembly = app.synth()
        dependencies = assembly.get_stack_artifact(stack.artifact_id).dependencies
        self.assertIn(processing.artifact_id, [dependency.id for dependency in dependencies])

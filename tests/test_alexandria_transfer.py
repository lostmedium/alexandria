import hashlib
import importlib.util
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote_plus

import boto3
from aws_cdk import App
from aws_cdk.assertions import Match, Template
from botocore.exceptions import ClientError

from test_alexandria_support import FakeCoordination, FakeS3
from alexandria.alexandria_coordination import AlexandriaCoordinationStack
from alexandria.alexandria_transfer import AlexandriaTransferStack


spec = importlib.util.spec_from_file_location(
    "transfer", Path(__file__).parents[1] / "transfer/transfer.py"
)
transfer = importlib.util.module_from_spec(spec)
with patch.object(boto3, "client"):
    spec.loader.exec_module(transfer)

MODEL = "global.anthropic.claude-haiku-5-5"


class FakeSQS:
    def __init__(self):
        self.messages = {}
        self.visibility = []

    def send_message(self, QueueUrl, MessageBody):
        self.messages.setdefault(QueueUrl, []).append(json.loads(MessageBody))

    def send_message_batch(self, QueueUrl, Entries):
        for entry in Entries:
            self.send_message(QueueUrl, entry["MessageBody"])
        return {"Successful": Entries}

    def change_message_visibility(self, **args):
        self.visibility.append(args)


class FakeDynamo:
    def __init__(self):
        self.items = {}

    def put_item(self, TableName, Item):
        self.items[(Item["pk"]["S"], Item["sk"]["S"])] = Item

    def get_item(self, TableName, Key, ConsistentRead):
        item = self.items.get((Key["pk"]["S"], Key["sk"]["S"]))
        return {"Item": item} if item else {}

    def transact_write_items(self, TransactItems):
        put, update = TransactItems[0]["Put"], TransactItems[1]["Update"]
        put_key = (put["Item"]["pk"]["S"], put["Item"]["sk"]["S"])
        run_key = (update["Key"]["pk"]["S"], update["Key"]["sk"]["S"])
        if put_key in self.items or run_key not in self.items:
            raise ClientError({
                "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                "CancellationReasons": [
                    {"Code": "ConditionalCheckFailed" if put_key in self.items else "None"},
                    {"Code": "None" if run_key in self.items else "ConditionalCheckFailed"},
                ],
            }, "TransactWriteItems")
        self.items[put_key] = put["Item"]
        run = self.items[run_key]
        run["done"] = {"N": str(int(run["done"]["N"]) + 1)}


class FakeBedrock:
    def __init__(self):
        self.calls = []
        self.response = None

    def converse(self, **args):
        self.calls.append(args)
        if isinstance(self.response, Exception):
            raise self.response
        if self.response:
            return self.response
        return {
            "stopReason": "end_turn",
            "output": {"message": {"content": [{"text": "### Topic\n\nDetail: 30 seconds.\n"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 5},
        }


class TransferTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.sqs = FakeSQS()
        self.dynamodb = FakeDynamo()
        self.bedrock = FakeBedrock()
        self.coordination = FakeCoordination()
        self.source_key = "guides/a.pdf"
        source = self.coordination.source_id(self.source_key)
        self.prefix = f"a/source-{source}__"
        self.manifest_key = self.prefix + "manifest.json"
        self.chunks = []
        for number in range(1, 4):
            body = (f"> Source: {self.source_key}\n> Section: Topic {number}\n"
                    f"> Chunk {number} of 3\n\n## Topic {number}\n\nText {number}.").encode()
            digest = hashlib.sha256(body).hexdigest()
            key = f"{self.prefix}revision-r__generation-g__part-000001-chunk-{number:06d}-{digest}.md"
            version = self.s3.put_object("staging", key, body, "text/markdown")["VersionId"]
            self.chunks.append({"key": key, "version_id": version, "sha256": digest})
        self.publish_manifest()
        for attribute, value in (
            ("s3", self.s3), ("sqs", self.sqs), ("dynamodb", self.dynamodb),
            ("bedrock", self.bedrock), ("coordination", self.coordination),
        ):
            change = patch.object(transfer, attribute, value)
            change.start()
            self.addCleanup(change.stop)
        environment = patch.dict(os.environ, {
            "STAGING_BUCKET": "staging", "ANALYZED_BUCKET": "analyzed",
            "TRACKING_TABLE": "tracking", "ANALYZE_QUEUE_URL": "analyze",
            "VERIFY_QUEUE_URL": "verify", "QUEUE_URL": "own", "MODEL_ID": MODEL,
            "MAX_OUTPUT_TOKENS": "16000",
        })
        environment.start()
        self.addCleanup(environment.stop)

    def publish_manifest(self):
        body = json.dumps({
            "source_key": self.source_key, "source_version": "v", "generation": "g",
            "revision": "r", "chunks": self.chunks,
        }).encode()
        return self.s3.put_object("staging", self.manifest_key, body, "application/json")["VersionId"]

    def analyzed(self, chunk, prompt=None):
        self.s3.put_object("analyzed", chunk["key"], b"done", "text/markdown", {
            "source-sha256": chunk["sha256"], "prompt-sha256": prompt or transfer.PROMPT_SHA256,
            "model-id": MODEL, "sha256": hashlib.sha256(b"done").hexdigest(),
        })

    def manifest_event(self, version):
        return {"Records": [{"body": json.dumps({"Records": [{"s3": {"object": {
            "key": quote_plus(self.manifest_key), "versionId": version,
        }}}]})}]}

    @staticmethod
    def sqs_event(body, number=0):
        return {"Records": [{
            "messageId": str(number), "receiptHandle": f"r{number}", "body": json.dumps(body),
        }]}

    def test_manifest_event_analyzes_pending_chunks_and_verifies_complete_pdf(self):
        self.analyzed(self.chunks[0])
        self.analyzed(self.chunks[1], prompt="0" * 64)
        self.s3.put_object("analyzed", self.prefix + "obsolete.md", b"old", "text/markdown")
        self.s3.put_object("analyzed", "other/untouched.md", b"other", "text/markdown")
        version = self.s3.objects[("staging", self.manifest_key)]["version"]
        [result] = transfer.transfer_handler(self.manifest_event(version), None)
        self.assertEqual((result["queued"], result["skipped"]), (2, 1))
        queued = self.sqs.messages["analyze"]
        self.assertEqual([m["chunk"]["key"] for m in queued],
                         [c["key"] for c in self.chunks[1:]])

        transfer.analyze_handler(self.sqs_event(queued[0], 1), None)
        self.assertNotIn("verify", self.sqs.messages)
        transfer.analyze_handler(self.sqs_event(queued[1], 2), None)
        transfer.analyze_handler(self.sqs_event(queued[0], 3), None)
        self.assertEqual(len(self.bedrock.calls), 2)
        run = self.dynamodb.items[(result["run"], "RUN")]
        self.assertEqual(run["done"]["N"], "2")

        call = self.bedrock.calls[0]
        self.assertEqual(call["modelId"], MODEL)
        self.assertEqual(call["additionalModelRequestFields"], {
            "thinking": {"type": "disabled"}, "output_config": {"effort": "high"},
        })
        self.assertEqual(call["system"][1], {"cachePoint": {"type": "default"}})
        self.assertTrue(call["messages"][0]["content"][0]["text"].startswith(
            f"<document>\n> Source: {self.source_key}\n"
        ))
        output = self.s3.objects[("analyzed", self.chunks[1]["key"])]
        self.assertTrue(output["body"].decode().startswith(
            f"> Source: {self.source_key}\n> Section: Topic 2\n> Chunk 2 of 3\n\n### Topic"
        ))
        self.assertEqual(output["metadata"]["source-sha256"], self.chunks[1]["sha256"])
        self.assertEqual(output["metadata"]["prompt-sha256"], transfer.PROMPT_SHA256)

        verify_message = self.sqs.messages["verify"][0]
        self.assertEqual(transfer.verify_handler(self.sqs_event(verify_message), None),
                         {"batchItemFailures": []})
        manifest = json.loads(self.s3.objects[("analyzed", self.manifest_key)]["body"])
        self.assertEqual([c["key"] for c in manifest["chunks"]], [c["key"] for c in self.chunks])
        self.assertEqual(manifest["staging_manifest_version"], version)
        self.assertNotIn(("analyzed", self.prefix + "obsolete.md"), self.s3.objects)
        self.assertIn(("analyzed", "other/untouched.md"), self.s3.objects)

    def test_manual_transfer_resumes_and_verification_reports_missing_chunks(self):
        self.analyzed(self.chunks[0])
        result = transfer.verify_handler({"source_key": self.source_key}, None)
        self.assertEqual((result["status"], result["missing"]), ("incomplete", 2))
        self.assertNotIn(("analyzed", self.manifest_key), self.s3.objects)
        result = transfer.transfer_handler({"source_key": self.source_key}, None)
        self.assertEqual(result["queued"], 2)
        for chunk in self.chunks[1:]:
            self.analyzed(chunk)
        result = transfer.transfer_handler({"source_key": self.source_key}, None)
        self.assertEqual(result["queued"], 0)
        self.assertEqual(len(self.sqs.messages["verify"]), 1)
        self.assertEqual(transfer.verify_handler({"source_key": self.source_key}, None)["status"],
                         "verified")

    def test_superseded_manifest_is_skipped(self):
        old = self.s3.objects[("staging", self.manifest_key)]["version"]
        self.publish_manifest()
        [result] = transfer.transfer_handler(self.manifest_event(old), None)
        self.assertEqual(result["status"], "superseded")
        self.assertEqual(self.sqs.messages, {})
        self.assertEqual(transfer.verify(self.manifest_key, old)["status"], "superseded")

    def test_incomplete_or_invalid_model_output_is_not_saved(self):
        transfer.transfer_handler({"source_key": self.source_key}, None)
        message = self.sqs.messages["analyze"][0]
        for response, error in (
            ({"stopReason": "max_tokens", "output": {"message": {"content": []}}}, "max_tokens"),
            ({"stopReason": "end_turn",
              "output": {"message": {"content": [{"text": "## Heading\n\ntext"}]}}}, "H1/H2"),
            ({"stopReason": "end_turn",
              "output": {"message": {"content": [{"text": "Here are the notes:\n\n### T\n\nx"}]}}},
             "topic heading"),
        ):
            self.bedrock.response = response
            with self.assertRaisesRegex(ValueError, error):
                transfer.analyze_handler(self.sqs_event(message), None)
        self.assertNotIn(("analyzed", message["chunk"]["key"]), self.s3.objects)

    def test_requests_stay_under_the_pricing_cliff(self):
        self.assertLessEqual(
            len(transfer.SYSTEM_PROMPT.encode()) + transfer.MAX_CHUNK_BYTES
            + transfer.REQUEST_OVERHEAD_TOKENS, transfer.PRICING_CLIFF_TOKENS,
        )
        body = b"x" * (transfer.MAX_CHUNK_BYTES + 1)
        digest = hashlib.sha256(body).hexdigest()
        key = f"{self.prefix}revision-r__generation-g__part-000001-chunk-000004-{digest}.md"
        version = self.s3.put_object("staging", key, body, "text/markdown")["VersionId"]
        self.chunks.append({"key": key, "version_id": version, "sha256": digest})
        self.publish_manifest()
        transfer.transfer_handler({"source_key": self.source_key}, None)
        with self.assertRaisesRegex(ValueError, "price tier"):
            transfer.analyze_handler(self.sqs_event(self.sqs.messages["analyze"][-1]), None)
        self.assertEqual(self.bedrock.calls, [])

        self.bedrock.response = {
            "stopReason": "end_turn",
            "output": {"message": {"content": [{"text": "### Topic\n\nDetail.\n"}]}},
            "usage": {"inputTokens": 99000, "cacheReadInputTokens": 2000, "outputTokens": 5},
        }
        with self.assertLogs(transfer.logger, "ERROR") as logs:
            transfer.analyze_handler(self.sqs_event(self.sqs.messages["analyze"][0]), None)
        self.assertIn("PricingCliff prompt_tokens=101000", logs.output[0])

    def test_abandoned_chunk_moves_to_dead_letter_queue_and_is_reported(self):
        transfer.transfer_handler({"source_key": self.source_key}, None)
        first, *rest = self.sqs.messages["analyze"]
        for number, message in enumerate(rest):
            transfer.analyze_handler(self.sqs_event(message, number), None)
        self.assertNotIn("verify", self.sqs.messages)
        self.bedrock.response = {"stopReason": "max_tokens", "output": {"message": {"content": []}}}
        event = self.sqs_event(first)
        with patch.dict(os.environ, {"FAILURE_LIMIT": "3", "DEAD_LETTER_QUEUE_URL": "dlq"}):
            event["Records"][0]["attributes"] = {"ApproximateReceiveCount": "2"}
            with self.assertRaisesRegex(ValueError, "max_tokens"):
                transfer.analyze_handler(event, None)
            event["Records"][0]["attributes"] = {"ApproximateReceiveCount": "3"}
            with self.assertLogs(transfer.logger, "ERROR"):
                self.assertEqual(transfer.analyze_handler(event, None), {"batchItemFailures": []})
        self.assertEqual(self.sqs.messages["dlq"], [first])
        with self.assertLogs(transfer.logger, "ERROR") as logs:
            result = transfer.verify_handler({"source_key": self.source_key}, None)
        self.assertEqual((result["status"], result["missing_keys"]),
                         ("incomplete", [first["chunk"]["key"]]))
        self.assertIn("AnalysisIncomplete", logs.output[0])
        self.assertEqual(len(self.sqs.messages["verify"]), 1)

    def test_throttling_defers_message(self):
        transfer.transfer_handler({"source_key": self.source_key}, None)
        self.bedrock.response = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "Converse",
        )
        result = transfer.analyze_handler(self.sqs_event(self.sqs.messages["analyze"][0], 7), None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "7"}]})
        self.assertEqual(self.sqs.visibility[0]["VisibilityTimeout"], 60)
        event = self.sqs_event(self.sqs.messages["analyze"][0], 8)
        event["Records"][0]["attributes"] = {"ApproximateReceiveCount": "60"}
        with patch.dict(os.environ, {"RETRY_LIMIT": "60", "DEAD_LETTER_QUEUE_URL": "dlq"}):
            with self.assertLogs(transfer.logger, "ERROR"):
                self.assertEqual(transfer.analyze_handler(event, None), {"batchItemFailures": []})
        self.assertEqual(len(self.sqs.messages["dlq"]), 1)


class TransferStackTest(unittest.TestCase):
    def test_transfer_resources(self):
        app = App()
        coordination = AlexandriaCoordinationStack(app, "Coordination")
        stack = AlexandriaTransferStack(
            app, "Transfer", staging_bucket_name="staging",
            coordination_table=coordination.table, coordination_layer=coordination.layer,
        )
        template = Template.from_stack(stack)
        template.has_resource_properties("AWS::S3::Bucket", {
            "VersioningConfiguration": {"Status": "Enabled"},
        })
        template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "transfer.analyze_handler",
            "Environment": {"Variables": Match.object_like({"MODEL_ID": MODEL})},
        })
        template.has_resource_properties("AWS::Lambda::EventSourceMapping", {
            "ScalingConfig": {"MaximumConcurrency": 50},
        })
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {"Statement": Match.array_with([Match.object_like({
                "Action": "bedrock:InvokeModel",
                "Resource": Match.array_with([
                    Match.object_like({"Fn::Join": Match.array_with([Match.array_with([
                        ":inference-profile/global.anthropic.claude-haiku-5-5",
                    ])])}),
                    Match.object_like({"Fn::Join": ["", [
                        "arn:", {"Ref": "AWS::Partition"},
                        ":bedrock:::foundation-model/anthropic.claude-haiku-5-5",
                    ]]}),
                ]),
            })])},
        })
        template.has_resource_properties("Custom::S3BucketNotifications", {
            "NotificationConfiguration": {"QueueConfigurations": [Match.object_like({
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": "__manifest.json"}]}},
            })]},
        })

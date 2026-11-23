import unittest
from unittest.mock import MagicMock, Mock, patch

from botocore.exceptions import ClientError
from aws_cdk import App
from aws_cdk.assertions import Template

from test_alexandria_support import FakeCoordination
import coordination
from alexandria.alexandria_coordination import AlexandriaCoordinationStack


class CoordinationTest(unittest.TestCase):
    def test_lock_releases_on_failure_and_aliases_reserved_attributes(self):
        table = Mock()
        with patch.object(coordination, "table", return_value=table):
            with self.assertRaisesRegex(ValueError, "failure"):
                with coordination.locked("source"):
                    raise ValueError("failure")
        acquire = table.put_item.call_args.kwargs
        release = table.delete_item.call_args.kwargs
        self.assertEqual(acquire["ExpressionAttributeNames"], {"#expires": "expires"})
        self.assertEqual(release["ExpressionAttributeNames"], {"#owner": "owner"})
        self.assertEqual(acquire["Item"]["owner"], release["ExpressionAttributeValues"][":owner"])
        self.assertGreaterEqual(acquire["Item"]["expires"], 1860)

    def test_contention_fails_instead_of_processing_unlocked(self):
        table = Mock()
        table.put_item.side_effect = ClientError({
            "Error": {"Code": "ConditionalCheckFailedException", "Message": "busy"},
        }, "PutItem")
        with patch.object(coordination, "table", return_value=table), \
             patch.object(coordination.time, "monotonic", side_effect=[0, 31]):
            with self.assertRaisesRegex(RuntimeError, "already in progress"):
                with coordination.locked("source"):
                    self.fail("A busy lock must not be acquired")
        table.delete_item.assert_not_called()

    def test_part_count_paginates(self):
        table = Mock()
        table.query.side_effect = [
            {"Count": 2, "LastEvaluatedKey": {"pk": "source", "sk": "part"}},
            {"Count": 1},
        ]
        with patch.object(coordination, "table", return_value=table):
            self.assertEqual(coordination.completed_parts("source", "generation"), 3)
        self.assertTrue(table.query.call_args.kwargs["ConsistentRead"])
        self.assertEqual(table.query.call_args.kwargs["ExclusiveStartKey"],
                         {"pk": "source", "sk": "part"})

    def test_removes_part_records_across_pages(self):
        table = MagicMock()
        table.query.side_effect = [
            {"Items": [{"pk": "source", "sk": "PART#one"}],
             "LastEvaluatedKey": {"pk": "source", "sk": "PART#one"}},
            {"Items": [{"pk": "source", "sk": "PART#two"}]},
        ]
        with patch.object(coordination, "table", return_value=table):
            coordination.clear_parts("source")
        writer = table.batch_writer.return_value.__enter__.return_value
        self.assertEqual(writer.delete_item.call_count, 2)
        self.assertEqual(table.query.call_args.kwargs["ExclusiveStartKey"],
                         {"pk": "source", "sk": "PART#one"})


class CoordinationStackTest(unittest.TestCase):
    def test_table_and_layer(self):
        template = Template.from_stack(AlexandriaCoordinationStack(App(), "Coordination"))
        template.has_resource_properties("AWS::DynamoDB::Table", {
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        })
        template.has_resource_properties("AWS::Lambda::LayerVersion", {
            "CompatibleRuntimes": ["python3.13"],
        })

import unittest

import aws_cdk as cdk
from aws_cdk.assertions import Template

from alexandria.alexandria_dynamodb import AlexandriaDynamodbStack


class AlexandriaDynamodbStackTest(unittest.TestCase):
    def test_table(self):
        app = cdk.App()
        template = Template.from_stack(AlexandriaDynamodbStack(app, "Test"))
        template.has_resource_properties("AWS::DynamoDB::Table", {
            "TableName": "pdf",
            "BillingMode": "PAY_PER_REQUEST",
            "DeletionProtectionEnabled": True,
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True},
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        })
        resources = template.to_json()["Resources"]
        table = next(resource for resource in resources.values()
                     if resource["Type"] == "AWS::DynamoDB::Table")
        self.assertEqual(table["DeletionPolicy"], "Retain")
        self.assertNotIn("Custom::AWS", [resource["Type"] for resource in resources.values()])


if __name__ == "__main__":
    unittest.main()

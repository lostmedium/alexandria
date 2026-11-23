from pathlib import Path

from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
)
from constructs import Construct


class AlexandriaCoordinationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table = dynamodb.Table(
            self, "Coordination",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.layer = lambda_.LayerVersion(
            self, "CoordinationLayer",
            code=lambda_.Code.from_asset(
                str(Path(__file__).parent.parent / "coordination"),
                exclude=["__pycache__", "*.pyc"],
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
        )

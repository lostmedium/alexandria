from pathlib import Path

from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
from constructs import Construct


class AlexandriaCleanupStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, *, cleanup_function_name: str,
        parts_bucket_name: str, markdown_bucket_name: str, chunks_bucket_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        outputs = [
            s3.Bucket.from_bucket_name(self, name, bucket_name)
            for name, bucket_name in (
                ("Parts", parts_bucket_name), ("Markdown", markdown_bucket_name),
                ("Chunks", chunks_bucket_name),
            )
        ]
        cleanup = lambda_.Function(
            self, "CleanupFunction",
            function_name=cleanup_function_name,
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="cleanup.lambda_handler",
            code=lambda_.Code.from_asset(
                str(Path(__file__).parent.parent / "cleanup"),
                exclude=["__pycache__", "*.pyc"],
            ),
            timeout=Duration.minutes(15),
            environment={
                "PARTS_BUCKET": parts_bucket_name, "MARKDOWN_BUCKET": markdown_bucket_name,
                "CHUNKS_BUCKET": chunks_bucket_name,
            },
        )
        for output in outputs:
            cleanup.add_to_role_policy(iam.PolicyStatement(
                actions=["s3:ListBucket"], resources=[output.bucket_arn],
            ))
            cleanup.add_to_role_policy(iam.PolicyStatement(
                actions=["s3:DeleteObject"], resources=[output.arn_for_objects("*")],
            ))

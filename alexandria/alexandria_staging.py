from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_s3_notifications as notifications,
    aws_sqs as sqs,
)
from constructs import Construct


class AlexandriaStagingStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, *, chunks_bucket_name: str,
        source_bucket_name: str, coordination_table: dynamodb.ITable,
        coordination_layer: lambda_.ILayerVersion, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        chunks = s3.Bucket.from_bucket_name(self, "Chunks", chunks_bucket_name)
        staging = s3.Bucket(
            self, "Staging",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
            lifecycle_rules=[s3.LifecycleRule(noncurrent_version_expiration=Duration.days(7))],
        )
        self.staging_bucket_name = staging.bucket_name
        sync = lambda_.Function(
            self, "StagingSyncFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="staging.lambda_handler",
            code=lambda_.Code.from_asset(
                str(Path(__file__).parent.parent / "staging"),
                exclude=["__pycache__", "*.pyc"],
            ),
            timeout=Duration.minutes(15),
            memory_size=1024,
            layers=[coordination_layer],
            environment={
                "CHUNKS_BUCKET": chunks.bucket_name, "STAGING_BUCKET": staging.bucket_name,
                "SOURCE_BUCKET": source_bucket_name,
                "COORDINATION_TABLE": coordination_table.table_name,
            },
        )
        chunks.grant_read(sync)
        staging.grant_read_write(sync)
        staging.grant_delete(sync)
        source = s3.Bucket.from_bucket_name(self, "PdfDownloads", source_bucket_name)
        source.grant_read(sync)
        coordination_table.grant_read_write_data(sync)
        dlq = sqs.Queue(
            self, "StagingDeadLetterQueue",
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.RETAIN,
        )
        queue = sqs.Queue(
            self, "StagingQueue",
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            visibility_timeout=Duration.minutes(90),
            retention_period=Duration.days(14),
            dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=100),
            removal_policy=RemovalPolicy.RETAIN,
        )
        sync.add_event_source(event_sources.SqsEventSource(queue, batch_size=1))
        chunks.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            notifications.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="__completion__/", suffix=".json"),
        )
        CfnOutput(self, "StagingBucketName", value=staging.bucket_name)
        CfnOutput(self, "StagingSyncFunctionName", value=sync.function_name)

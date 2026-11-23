from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as notifications,
    aws_sqs as sqs,
)
from constructs import Construct


class AlexandriaProcessingStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, *, download_bucket_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket.from_bucket_name(self, "PdfDownloads", download_bucket_name)
        parts = s3.Bucket(
            self, "PdfParts",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        markdown = s3.Bucket(
            self, "Markdown",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        chunks = s3.Bucket(
            self, "Chunks",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        for name, source, destination, handler, suffix in [
            ("Split", bucket, parts, "processing.split_handler", None),
            ("Convert", parts, markdown, "processing.convert_handler", ".pdf"),
            ("Chunk", markdown, chunks, "processing.chunk_handler", ".md"),
        ]:
            dlq = sqs.Queue(
                self, f"{name}DeadLetterQueue",
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                retention_period=Duration.days(14),
                removal_policy=RemovalPolicy.RETAIN,
            )
            queue = sqs.Queue(
                self, f"{name}Queue",
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                visibility_timeout=Duration.minutes(90),
                retention_period=Duration.days(14),
                dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=5),
                removal_policy=RemovalPolicy.RETAIN,
            )
            processing_logs = logs.LogGroup(
                self, f"{name}Logs",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.RETAIN,
            )
            function = lambda_.DockerImageFunction(
                self, f"{name}Function",
                code=lambda_.DockerImageCode.from_image_asset(
                    str(Path(__file__).parent.parent / "processing"),
                    cmd=[handler],
                ),
                timeout=Duration.minutes(15),
                memory_size=4096,
                ephemeral_storage_size=Size.gibibytes(10),
                environment={"DESTINATION_BUCKET": destination.bucket_name},
                log_group=processing_logs,
            )
            source.grant_read(function)
            destination.grant_put(function)
            function.add_event_source(event_sources.SqsEventSource(queue, batch_size=1))
            filters = [s3.NotificationKeyFilter(suffix=suffix)] if suffix else []
            source.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                notifications.SqsDestination(queue),
                *filters,
            )

        CfnOutput(self, "PartsBucketName", value=parts.bucket_name)
        CfnOutput(self, "MarkdownBucketName", value=markdown.bucket_name)
        CfnOutput(self, "ChunkBucketName", value=chunks.bucket_name)

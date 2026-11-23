from pathlib import Path

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct


class AlexandriaDownloadStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = dynamodb.Table.from_table_name(self, "PdfTable", "pdf")
        bucket = s3.Bucket(
            self, "PdfDownloads",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        role = iam.Role(
            self, "DownloadRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        log_group = logs.LogGroup(
            self, "DownloadLogs",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )
        log_group.grant_write(role)
        role.add_to_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:UpdateItem"],
            resources=[table.table_arn],
        ))
        bucket.grant_put(role)

        download = lambda_.Function(
            self, "DownloadFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            architecture=lambda_.Architecture.ARM_64,
            handler="download.lambda_handler",
            code=lambda_.Code.from_asset(
                str(Path(__file__).parent.parent / "download"),
                exclude=["__pycache__", "*.pyc"],
            ),
            role=role,
            log_group=log_group,
            timeout=Duration.minutes(15),
            memory_size=1024,
            ephemeral_storage_size=Size.gibibytes(1),
            reserved_concurrent_executions=1,
            environment={"TABLE_NAME": table.table_name, "BUCKET_NAME": bucket.bucket_name},
        )
        events.Rule(
            self, "DailyDownload",
            schedule=events.Schedule.cron(minute="0", hour="0"),
            targets=[targets.LambdaFunction(download)],
        )

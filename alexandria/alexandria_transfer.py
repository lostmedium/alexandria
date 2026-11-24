from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as notifications,
    aws_sqs as sqs,
)
from constructs import Construct


MODEL_ID = "global.anthropic.claude-haiku-5-5"
FOUNDATION_MODEL = "anthropic.claude-haiku-5-5"


class AlexandriaTransferStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, *, staging_bucket_name: str,
        coordination_table: dynamodb.ITable, coordination_layer: lambda_.ILayerVersion,
        analyze_concurrency: int = 50, max_output_tokens: int = 16000, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        staging = s3.Bucket.from_bucket_name(self, "Staging", staging_bucket_name)
        analyzed = s3.Bucket(
            self, "Analyzed",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
            lifecycle_rules=[s3.LifecycleRule(noncurrent_version_expiration=Duration.days(7))],
        )
        tracking = dynamodb.Table(
            self, "Tracking",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires",
            removal_policy=RemovalPolicy.DESTROY,
        )

        queues, dead_letter_queues = {}, {}
        # Analyze moves messages to its DLQ itself (RETRY_LIMIT, FAILURE_LIMIT); 100 is a backstop.
        for name, receives in (("Transfer", 5), ("Analyze", 100), ("Verify", 100)):
            dlq = sqs.Queue(
                self, f"{name}DeadLetterQueue",
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                retention_period=Duration.days(14),
                removal_policy=RemovalPolicy.RETAIN,
            )
            dead_letter_queues[name] = dlq
            queues[name] = sqs.Queue(
                self, f"{name}Queue",
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                visibility_timeout=Duration.minutes(90),
                retention_period=Duration.days(14),
                dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=receives),
                removal_policy=RemovalPolicy.RETAIN,
            )

        # Global CRIS authorizes the profile, the source-Region model and the Region-less model.
        model_resources = [
            self.format_arn(service="bedrock", resource="inference-profile", resource_name=MODEL_ID),
            self.format_arn(
                service="bedrock", account="", resource="foundation-model",
                resource_name=FOUNDATION_MODEL,
            ),
            self.format_arn(
                service="bedrock", region="", account="", resource="foundation-model",
                resource_name=FOUNDATION_MODEL,
            ),
        ]
        functions = {}
        for name, handler in (
            ("Transfer", "transfer.transfer_handler"),
            ("Analyze", "transfer.analyze_handler"),
            ("Verify", "transfer.verify_handler"),
        ):
            function_logs = logs.LogGroup(
                self, f"{name}Logs",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.RETAIN,
            )
            function = lambda_.Function(
                self, f"{name}Function",
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler=handler,
                code=lambda_.Code.from_asset(
                    str(Path(__file__).parent.parent / "transfer"),
                    exclude=["__pycache__", "*.pyc"],
                ),
                timeout=Duration.minutes(15),
                memory_size=1024,
                layers=[coordination_layer],
                environment={
                    "STAGING_BUCKET": staging.bucket_name,
                    "ANALYZED_BUCKET": analyzed.bucket_name,
                    "TRACKING_TABLE": tracking.table_name,
                    "COORDINATION_TABLE": coordination_table.table_name,
                    "ANALYZE_QUEUE_URL": queues["Analyze"].queue_url,
                    "VERIFY_QUEUE_URL": queues["Verify"].queue_url,
                    "QUEUE_URL": queues[name].queue_url,
                    "MODEL_ID": MODEL_ID,
                    "MAX_OUTPUT_TOKENS": str(max_output_tokens),
                },
                log_group=function_logs,
            )
            staging.grant_read(function)
            analyzed.grant_read(function)
            functions[name] = function

        transfer, analyze, verify = (functions[name] for name in ("Transfer", "Analyze", "Verify"))
        tracking.grant_read_write_data(transfer)
        tracking.grant_read_write_data(analyze)
        queues["Analyze"].grant_send_messages(transfer)
        queues["Verify"].grant_send_messages(transfer)
        queues["Verify"].grant_send_messages(analyze)
        analyzed.grant_put(analyze)
        analyze.add_environment("FAILURE_LIMIT", "3")
        analyze.add_environment("RETRY_LIMIT", "60")
        analyze.add_environment("DEAD_LETTER_QUEUE_URL", dead_letter_queues["Analyze"].queue_url)
        dead_letter_queues["Analyze"].grant_send_messages(analyze)
        analyze.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"], resources=model_resources,
        ))
        analyzed.grant_put(verify)
        analyzed.grant_delete(verify)
        coordination_table.grant_read_write_data(verify)

        transfer.add_event_source(event_sources.SqsEventSource(queues["Transfer"], batch_size=1))
        analyze.add_event_source(event_sources.SqsEventSource(
            queues["Analyze"], batch_size=1, report_batch_item_failures=True,
            max_concurrency=analyze_concurrency,
        ))
        verify.add_event_source(event_sources.SqsEventSource(
            queues["Verify"], batch_size=1, report_batch_item_failures=True,
        ))
        staging.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            notifications.SqsDestination(queues["Transfer"]),
            s3.NotificationKeyFilter(suffix="__manifest.json"),
        )

        CfnOutput(self, "AnalyzedBucketName", value=analyzed.bucket_name)
        CfnOutput(self, "TransferFunctionName", value=transfer.function_name)
        CfnOutput(self, "VerifyFunctionName", value=verify.function_name)

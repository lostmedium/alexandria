#!/usr/bin/env python3
import os

import aws_cdk as cdk

from alexandria.alexandria_stack import AlexandriaStack
from alexandria.alexandria_dynamodb import AlexandriaDynamodbStack
from alexandria.alexandria_download import AlexandriaDownloadStack
from alexandria.alexandria_processing import AlexandriaProcessingStack
from alexandria.alexandria_cleanup import AlexandriaCleanupStack
from alexandria.alexandria_staging import AlexandriaStagingStack
from alexandria.alexandria_coordination import AlexandriaCoordinationStack

app = cdk.App()

AlexandriaStack(
    app, "AlexandriaStack",
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

pdf_table_stack = AlexandriaDynamodbStack(
    app, "AlexandriaDynamodbStack",
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

coordination_stack = AlexandriaCoordinationStack(
    app, "AlexandriaCoordinationStack",
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

download_stack = AlexandriaDownloadStack(
    app, "AlexandriaDownloadStack",
    coordination_table = coordination_stack.table,
    coordination_layer = coordination_stack.layer,
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)
download_stack.add_stack_dependency(pdf_table_stack)

processing_stack = AlexandriaProcessingStack(
    app, "AlexandriaProcessingStack",
    download_bucket_name = download_stack.bucket_name,
    cleanup_function_name = download_stack.cleanup_function_name,
    coordination_table = coordination_stack.table,
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

AlexandriaCleanupStack(
    app, "AlexandriaCleanupStack",
    cleanup_function_name = download_stack.cleanup_function_name,
    parts_bucket_name = processing_stack.parts_bucket_name,
    markdown_bucket_name = processing_stack.markdown_bucket_name,
    chunks_bucket_name = processing_stack.chunks_bucket_name,
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

AlexandriaStagingStack(
    app, "AlexandriaStagingStack",
    chunks_bucket_name = processing_stack.chunks_bucket_name,
    source_bucket_name = download_stack.bucket_name,
    coordination_table = coordination_stack.table,
    coordination_layer = coordination_stack.layer,
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

cdk.Tags.of(app).add('Alias','alexandria')
cdk.Tags.of(app).add('Domain','lostmedium.com')
cdk.Tags.of(app).add('GitHub','https://github.com/lostmedium/alexandria')

app.synth()
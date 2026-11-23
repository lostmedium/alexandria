#!/usr/bin/env python3
import os

import aws_cdk as cdk

from alexandria.alexandria_stack import AlexandriaStack
from alexandria.alexandria_dynamodb import AlexandriaDynamodbStack
from alexandria.alexandria_download import AlexandriaDownloadStack
from alexandria.alexandria_processing import AlexandriaProcessingStack

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

download_stack = AlexandriaDownloadStack(
    app, "AlexandriaDownloadStack",
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)
download_stack.add_stack_dependency(pdf_table_stack)

AlexandriaProcessingStack(
    app, "AlexandriaProcessingStack",
    download_bucket_name = download_stack.bucket_name,
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
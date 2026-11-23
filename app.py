#!/usr/bin/env python3
import os

import aws_cdk as cdk

from alexandria.alexandria_stack import AlexandriaStack

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

cdk.Tags.of(app).add('Alias','alexandria')
cdk.Tags.of(app).add('Domain','lostmedium.com')
cdk.Tags.of(app).add('GitHub','https://github.com/lostmedium/alexandria')

app.synth()
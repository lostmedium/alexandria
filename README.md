# alexandria

The CDK app includes `AlexandriaDynamodbStack`, which creates the `pdf`
DynamoDB table in `us-east-2` with string partition key `pk` and sort key `sk`.
Records are added manually; use `PDF#` as the partition key and
`PDF#<fname>` as the sort key. The table is retained when the stack is deleted.
Point-in-time recovery and deletion protection are enabled; recovery incurs
additional charges. Disable deletion protection before intentionally deleting
the table.

For the IAM user guide, add an item with these string attributes:

```json
{
  "pk": "PDF#",
  "sk": "PDF#iam-ug.pdf",
  "name": "AWS Identity and Access Management",
  "fname": "iam-ug.pdf",
  "link": "https://docs.aws.amazon.com/IAM/latest/UserGuide/iam-ug.pdf",
  "rss": "https://docs.aws.amazon.com/IAM/latest/UserGuide/aws-iam-release-notes.rss",
  "lastupdate": "TBD"
}
```

Deploy with `cdk deploy AlexandriaDynamodbStack AlexandriaDownloadStack`. A
retained `pdf` table must be handled before creating a replacement stack with
the same table name.

The download stack uses the existing `pdf` table and creates a private,
retained S3 bucket and a Lambda function scheduled daily at 00:00 UTC. The function
can also be invoked manually from the Lambda console or AWS CLI with an empty
event (`{}`). Each record's `rss` must be an RSS 2.0 feed with dated items;
the most recent item's `pubDate` is compared with `lastupdate`. When it differs,
the PDF at `link` is uploaded under the `fname` S3 key and `lastupdate` is set
to that RSS date only after the upload succeeds. Reusing a `fname` overwrites
the existing object. Failed records are logged and cause the invocation to fail;
successful records are skipped on retry. Downloads use temporary storage, which
is cleaned after each invocation and before the next one after an interruption.
The ARM64 function uses the Lambda runtime's `urllib3` and boto3, with no requests layer.
The local `boto3` requirement is used by the downloader tests; it is not bundled
with the Lambda.
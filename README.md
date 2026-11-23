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

Deploy with `cdk deploy AlexandriaDynamodbStack AlexandriaDownloadStack AlexandriaProcessingStack`. A
retained `pdf` table must be handled before creating a replacement stack with
the same table name.

If the earlier combined download/processing stack was deployed, moving the
processing resources creates new buckets and queues; retained data and pending
messages are not migrated automatically. Inspect and handle them before
replacing the old processing resources.

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

## PDF preparation

`AlexandriaProcessingStack` uses the download stack's bucket and prepares
uploaded PDFs automatically:

```
downloads bucket -> SQS -> split PDF -> parts bucket -> SQS -> Markdown
                 -> Markdown bucket -> SQS -> chunked Markdown bucket
```

Only object-created events start processing; non-PDF source keys are ignored.
Existing objects do not trigger processing on deployment: upload them again if
they should be processed. S3 versioning is not enabled on any of the buckets.
If the source key is overwritten before a queued event is processed, its ETag
check fails rather than processing the replacement as the earlier upload. The
event retries and eventually reaches the dead-letter queue; the replacement
upload has its own event.

PDFs are split into parts of at most 1,000 pages using PyMuPDF and converted
using pymupdf4llm without forced OCR. Markdown is split at headings, paragraphs,
lines and finally characters, aiming for 15,000 characters before provenance
and continuation headings are added. Tables and fenced code blocks remain
together when they fit. This follows the preparation approach in
[scs](https://github.com/jblukach/scs); it does not deploy or invoke any AI,
Bedrock, inference, or assembly stage.

The download and processing stacks output their bucket names. In each output
bucket, files are grouped under one folder named for the source filename
without its extension. Revision, part and source-key identifiers are in each
filename. For example, `guides/My guide.pdf` produces:

```
parts bucket:    My guide/source-<source-key-hash>__revision-<etag-hash>__part-000001.pdf
Markdown bucket: My guide/source-<source-key-hash>__revision-<etag-hash>__part-000001.md
chunks bucket:   My guide/source-<source-key-hash>__revision-<etag-hash>__part-000001-chunk-000001.md
```

Each bucket identifies its stage, so no additional `parts`, `markdown` or
`chunks` folder is needed. The source-key hash distinguishes same-named PDFs
from different source paths, and the revision hash keeps different uploads
separate. The original source key is also retained in object metadata and in
chunk provenance.

The revision hash is SHA-256 of the event ETag. Each chunk also includes its
original source key, part, document title and section when available, and chunk
position. Different ETags produce separate output sets even though S3 bucket
versioning is disabled; old parts, Markdown and chunks are not automatically
deleted. Retries can overwrite objects for the same ETag.

Chunked Markdown targets 15,000 characters of content, following SCS's
`text2chunk` limit; continuation and provenance headings are added afterward,
and sizes are measured in characters, not tokens. SCS stages full groups of
100 chunks separately from the remaining 0-99, but does not define different
chunk sizes for different AI model tiers. This stack stops at the chunked
Markdown bucket and does not stage or invoke those AI-processing steps.
All four buckets and the queues are retained on stack deletion.

Each stage has a 15-minute timeout, 4 GiB memory and 10 GiB temporary storage.
Large or complex PDFs can still exceed these limits, including during splitting
or conversion of a 1,000-page part. Image-only PDFs may not yield usable text;
empty extraction fails rather than producing an empty chunk. Partial results
may already be present when a stage fails; the presence of a chunk alone does
not prove that the entire PDF finished. Temporary working files are cleaned
after each operation and after an interrupted invocation when processing resumes.

Each queue retains messages for 14 days, uses a 90-minute visibility timeout,
and sends messages to its own dead-letter queue after five unsuccessful
receives. Monitor all three queues, dead-letter queues and Lambda logs; there
are no configured notifications or completion manifest. Investigate and redrive
failed messages before they expire, and verify all parts and chunks before
using the results. Reprocessing can generate duplicate events; the same ETag
uses the same output keys.

Building/deploying the processing images requires a running Docker daemon.
Run local tests with `python -m unittest discover -s tests` after installing
`requirements.txt`. PDF processing dependencies are also used by the image.
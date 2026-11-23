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

Deploy with `cdk deploy AlexandriaDynamodbStack AlexandriaCoordinationStack
AlexandriaDownloadStack AlexandriaProcessingStack AlexandriaCleanupStack
AlexandriaStagingStack`. A
retained `pdf` table must be handled before creating a replacement stack with
the same table name. Deploy outside the download schedule, and ensure the
cleanup stack is ready before the downloader runs.

If the earlier combined download/processing stack was deployed, moving the
processing resources creates new buckets and queues; retained data and pending
messages are not migrated automatically. Inspect and handle them before
replacing the old processing resources.

If cleanup and staging were already deployed inside `AlexandriaProcessingStack`,
pause scheduled downloads, deploy the updated processing stack to remove those
resources, and then deploy the cleanup and staging stacks. The old staging
bucket is retained but is not moved or automatically migrated; the new staging
stack creates a new bucket. Resume downloads only after cleanup is deployed.

The download stack uses the existing `pdf` table and creates a private,
retained S3 bucket and a Lambda function scheduled daily at 00:00 UTC. The function
can also be invoked manually from the Lambda console or AWS CLI with an empty
event (`{}`). Each record's `rss` must be an RSS 2.0 feed with dated items;
the most recent item's `pubDate` is compared with `lastupdate`. When it differs,
the PDF at `link` is uploaded under the `fname` S3 key and `lastupdate` is set
to that RSS date only after the upload succeeds. Before downloading, a cleanup
function deletes current parts, Markdown and chunks for that exact source key.
If cleanup or download fails, the date is not updated, but previously deleted
outputs are not automatically restored. Reusing a `fname` overwrites the
existing object. Failed records are logged and cause the invocation to fail;
successful records are skipped on retry. Downloads use temporary storage, which
is cleaned after each invocation and before the next one after an interruption.
The ARM64 function uses the Lambda runtime's `urllib3` and boto3, with no requests layer.
The cleanup and staging stacks must be deployed before an updated download
function runs. The local `boto3` requirement is used by the downloader tests; it
is not bundled with the Lambda.

## PDF preparation

`AlexandriaProcessingStack` uses the download stack's bucket and prepares
uploaded PDFs automatically:

```
downloads bucket -> SQS -> split PDF -> parts bucket -> SQS -> Markdown
                 -> Markdown bucket -> SQS -> chunked Markdown bucket
```

Only object-created events start processing; non-PDF source keys are ignored.
Before deploying this coordination change, drain the processing queues and
wait for in-flight work to finish. Legacy part and Markdown events do not
contain a generation identifier and are skipped. Existing PDFs do not trigger
processing on deployment: upload them again to generate completion markers and
staging manifests. When a PDF is overwritten, events from older versions are
skipped rather than processing them as the replacement.

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
parts bucket:    My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001.pdf
Markdown bucket: My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001.md
chunks bucket:   My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001-chunk-000001-<chunk-hash>.md
```

Each bucket identifies its stage, so no additional `parts`, `markdown` or
`chunks` folder is needed. The source-key hash distinguishes same-named PDFs
from different source paths, and the revision hash keeps different uploads
separate. The generation hash identifies the specific S3 version of the PDF.
The original source key is also retained in object metadata and in chunk provenance.

The revision hash is SHA-256 of the PDF bytes, and the chunk hash is SHA-256
of the entire chunk, including provenance. Each chunk also includes its
original source key, part, document title and section when available, and chunk
position. The ETag is still used to ensure the downloaded PDF matches its
S3 event. Retries can overwrite objects for the same content.

Chunked Markdown targets 15,000 characters of content, following SCS's
`text2chunk` limit; continuation and provenance headings are added afterward,
and sizes are measured in characters, not tokens. This stack does not invoke
AI-processing steps.

## Staging and retention

The coordination stack tracks each PDF's generation and completed parts in
DynamoDB. Once all parts have been chunked, a completion marker under
`__completion__/` in the chunks bucket sends a message to the staging queue.
There is no scheduled staging rule. The staging Lambda can also be invoked
manually with `{"source_key": "guides/My guide.pdf"}` after processing finishes.

Staging copies the completed PDF's chunks and then atomically writes
`My guide/source-<source-key-hash>__manifest.json` in the staging bucket. The
manifest identifies the source PDF version, generation, PDF hash and each
chunk's key, S3 version ID and SHA-256 hash. **Read the manifest**, not a bucket
listing: chunks copied during an unsuccessful attempt may remain unreferenced.
Consumers should retrieve each referenced object using its version ID, so a
reader holding an older manifest can still read its chunks until the seven-day
noncurrent-version retention expires; this requires `s3:GetObjectVersion`
permission. For example, a manifest has this shape:

```json
{
  "source_key": "guides/My guide.pdf",
  "source_version": "<PDF S3 version ID>",
  "generation": "<generation hash>",
  "revision": "<PDF SHA-256>",
  "chunks": [
    {"key": "My guide/source-...-chunk-000001-<chunk-hash>.md",
     "version_id": "<staged S3 version ID>", "sha256": "<chunk SHA-256>"}
  ]
}
```

After publishing the new manifest,
staging deletes obsolete current chunks for that PDF, without touching other
PDFs. If processing or copying fails, its previous manifest remains current.

The staging Lambda has a 15-minute timeout. Failed messages retry and may reach
the staging dead-letter queue; monitor and redrive it. Reinvocation skips
already-copied chunks. A PDF whose staging takes longer than one invocation
cannot be published until its copy can finish within that timeout.

All five application buckets have versioning enabled and expire noncurrent
object versions seven days after they become noncurrent. Current objects do not
expire under these rules. Deletes create delete markers; older versions remain
recoverable only until the lifecycle policy expires them. All buckets and the
queues are retained on stack deletion.

Each stage has a 15-minute timeout, 4 GiB memory and 10 GiB temporary storage.
Large or complex PDFs can still exceed these limits, including during splitting
or conversion of a 1,000-page part. Image-only PDFs may not yield usable text;
empty extraction fails rather than producing an empty chunk. Partial results
may already be present when a stage fails; the presence of a chunk alone does
not prove that the entire PDF finished. Temporary working files are cleaned
after each operation and after an interrupted invocation when processing resumes.

Each processing and staging queue retains messages for 14 days, uses a 90-minute
visibility timeout, and sends messages to its own dead-letter queue after 100
unsuccessful receives. Monitor all queues, dead-letter queues and Lambda logs;
there are no configured alerts. Investigate and redrive failed messages before
they expire.

Downloads, processing stages and staging use a per-source DynamoDB lease, so
operations for the same PDF are serialized; different PDFs can proceed in
parallel. A busy lease causes the message or download to retry. An interrupted
invocation can hold a lease for up to 31 minutes. The download function clears
old outputs before downloading; the splitter also clears old outputs before
processing a new version, including a manual upload. Old queued events are
ignored. A failed download leaves the prior staging manifest intact, while the
old PDF version is blocked from being processed as a new update.

Building/deploying the processing images requires a running Docker daemon.
Run local tests with `python -m unittest discover -s tests` after installing
`requirements.txt`. PDF processing dependencies are also used by the image.
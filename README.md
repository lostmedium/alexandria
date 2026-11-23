# alexandria

Alexandria downloads PDFs, converts them to Markdown, and prepares chunks for
AI processing. It stages a completed PDF's chunks and a manifest in S3; it
does not run an AI model.

The downloader checks each PDF's RSS feed daily at 00:00 UTC. S3 events and SQS
queues start processing when a PDF is uploaded. Staging runs after all parts of
that PDF have been chunked, rather than on a schedule.

## Deployment

The CDK app deploys to `us-east-2` using the `lukach` bootstrap qualifier.
Deployment requires Python, the AWS CDK CLI, AWS credentials, and a running
Docker daemon for the processing images. Install the Python dependencies with
`python -m pip install -r requirements.txt`, then deploy:

```sh
cdk deploy AlexandriaDynamodbStack AlexandriaCoordinationStack \
  AlexandriaDownloadStack AlexandriaProcessingStack AlexandriaCleanupStack \
  AlexandriaStagingStack
```

Deploy outside the download schedule. The cleanup and staging stacks must be
ready before the downloader runs. The `pdf` table is retained when its stack is
deleted; handle any retained table before creating a replacement with the same
name.

### Upgrading an existing deployment

Drain the processing queues and wait for in-flight work to finish before
deploying the coordination changes. Events for older parts and Markdown files
have no generation identifier and are skipped. Existing PDFs do not trigger
processing on deployment; upload them again to create completion markers and
staging manifests.

If upgrading from a combined download/processing stack, the new processing
buckets and queues do not automatically receive retained data or pending
messages. If cleanup and staging were deployed inside
`AlexandriaProcessingStack`, pause downloads and deploy the updated processing
stack before deploying the separate cleanup and staging stacks. The old
staging bucket is retained but is not automatically migrated.

## PDF sources

`AlexandriaDynamodbStack` creates the `pdf` table with string partition key `pk`
and sort key `sk`. Add records manually using `PDF#` as the partition key and
`PDF#<fname>` as the sort key. Point-in-time recovery and deletion protection
are enabled; recovery incurs additional charges. Disable deletion protection
before intentionally deleting the table.

For example, add the IAM user guide with these string attributes:

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

Each `rss` must be an RSS 2.0 feed with dated items. The downloader compares
the latest `pubDate` with `lastupdate`. If they differ, it deletes the current
parts, Markdown and chunks for that source key, downloads the PDF at `link`,
and uploads it under `fname`. It updates `lastupdate` only after the upload
succeeds. The downloader can also be invoked manually with an empty event
(`{}`).

If cleanup or download fails, the date is not updated, but deleted outputs are
not automatically restored. Failed records are logged and cause the invocation
to fail; successful records are skipped on retry. Temporary files are cleaned
after each invocation and before the next one after an interruption. The ARM64
function uses the Lambda runtime's `urllib3` and boto3, not a requests layer.

## PDF preparation

`AlexandriaProcessingStack` uses the download stack's bucket and prepares
uploaded PDFs automatically:

```
downloads bucket -> SQS -> split PDF -> parts bucket -> SQS -> Markdown
                 -> Markdown bucket -> SQS -> chunked Markdown bucket
```

Only object-created events start processing; non-PDF source keys are ignored.
When a PDF is overwritten, events from older versions are skipped rather than
processing them as the replacement.

PDFs are split into parts of at most 1,000 pages using PyMuPDF and converted
using pymupdf4llm without forced OCR. Markdown is split at headings, paragraphs,
lines and finally characters, aiming for 15,000 characters before provenance
and continuation headings are added. Tables and fenced code blocks remain
together when they fit. Sizes are measured in characters, not tokens.

The download and processing stacks output their bucket names. In each output
bucket, files are grouped under one folder named for the source filename
without its extension. Revision, part and source-key identifiers are in each
filename. For example, `guides/My guide.pdf` produces:

```
parts bucket:    My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001.pdf
Markdown bucket: My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001.md
chunks bucket:   My guide/source-<source-key-hash>__revision-<pdf-hash>__generation-<generation-hash>__part-000001-chunk-000001-<chunk-hash>.md
```

The source-key hash distinguishes same-named PDFs from different paths. The
revision hash is SHA-256 of the PDF bytes; identical PDFs have the same revision
hash. The generation hash is SHA-256 of the source S3 version ID, so separate
uploads have different generations even when their content is identical. The
chunk hash is SHA-256 of the entire chunk, including provenance.

Parts and Markdown retain the original source key as base64-encoded object
metadata. Each chunk includes the source key, part, document title and section
when available, and chunk position in its text; its metadata contains the
chunk hash. The splitter uses the event ETag to check the downloaded PDF.

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

After publishing the new manifest, staging deletes obsolete current chunks for
that PDF, without touching other PDFs. If processing or copying fails, its
previous manifest remains current.

The staging Lambda has a 15-minute timeout. Failed messages retry and may reach
the staging dead-letter queue; monitor and redrive it. Retries reuse
already-copied chunks, but the final pass must finish within the timeout to
publish a manifest.

All five application buckets have versioning enabled. Noncurrent object versions
become eligible for expiration seven days after they become noncurrent; current
objects do not expire under these rules. Deletes create delete markers, and
older versions remain recoverable until the lifecycle policy expires them.
Buckets and queues are retained on stack deletion.

Each PDF processing function has a 15-minute timeout, 4 GiB memory and 10 GiB
temporary storage. Large or complex PDFs can still exceed these limits,
including during splitting or conversion of a 1,000-page part. Image-only PDFs
may not yield usable text; empty extraction fails rather than producing an empty
chunk. Partial results may already be present when a stage fails; the presence
of a chunk alone does not prove that the entire PDF finished. Temporary working
files are cleaned after each operation and after an interrupted invocation when
processing resumes.

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

Run local tests with `python -m unittest discover -s tests`.
# alexandria

Alexandria downloads PDFs, converts them to Markdown, and prepares chunks for
AI processing. It stages a completed PDF's chunks and a manifest in S3, then
analyzes every staged chunk with Claude Haiku 5.5 on Amazon Bedrock and writes
the verified results to an analyzed bucket.

The downloader checks each PDF's RSS feed daily at 00:00 UTC. S3 events and SQS
queues start processing when a PDF is uploaded. Staging runs after all parts of
that PDF have been chunked, rather than on a schedule. Analysis runs after
staging publishes that PDF's manifest.

## Deployment

The CDK app deploys to `us-east-2` using the `lukach` bootstrap qualifier.
Deployment requires Python, the AWS CDK CLI, AWS credentials, and a running
Docker daemon for the processing images. Install the Python dependencies with
`python -m pip install -r requirements.txt`, then deploy:

```sh
cdk deploy AlexandriaDynamodbStack AlexandriaCoordinationStack \
  AlexandriaDownloadStack AlexandriaProcessingStack AlexandriaCleanupStack \
  AlexandriaStagingStack AlexandriaTransferStack
```

Deploy outside the download schedule. The cleanup, staging and transfer stacks must
be ready before the downloader runs. The `pdf` table is retained when its stack is
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

When upgrading the processing performance changes, deploy the coordination
layer and processing stack together with their dependent download and staging
stacks. Drain queues and wait for in-flight work first. Already-split versions
keep their existing part sizes; upload a PDF as a new S3 version to use 50-page
parts. Completed part records created before chunk-count tracking cannot be
verified for staging; re-upload those PDFs to regenerate their records and
chunks.

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

PDFs are split into parts of at most 50 pages using PyMuPDF, without expensive
deduplication or recompression. Parts convert in parallel using pymupdf4llm's
native text/table extraction, with ML layout analysis and OCR disabled. This
fast path can differ from ML layout extraction on complex page layouts.
Each uploaded part can convert and chunk while the splitter prepares later
parts; conversion does not wait for all part uploads.
Markdown is split at headings, paragraphs,
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
hash. The generation hash is SHA-256 of the source S3 version ID and a unique
split-attempt identifier. Retrying an interrupted split starts a fresh generation
and clears partial outputs, so in-flight results from the failed attempt cannot
publish into the retry, even for the same PDF version. Duplicate events for an
already-split version reuse its generation. The chunk hash is SHA-256 of the
entire chunk, including provenance.

Parts and Markdown retain the original source key as base64-encoded object
metadata. Each chunk includes the source key, part, document title and section
when available, and chunk position in its text; its metadata contains the
chunk hash. The splitter uses the event ETag to check the downloaded PDF.
Duplicate conversion events reuse Markdown for the same part S3 version rather
than extracting again. Chunking reuses existing content-addressed chunks and
uploads missing chunks using up to 16 concurrent S3 requests. A new split
generation uses new chunk keys even if its text is unchanged.

## Staging and retention

The coordination stack tracks each PDF's generation and completed parts in
DynamoDB. Once splitting has finished and all expected parts have been chunked,
a completion marker under `__completion__/` in the chunks bucket sends a
message to the staging queue. Before publishing, staging checks the recorded
chunk count and sequence for every part; missing chunks fail staging instead
of producing an incomplete manifest. Reprocess the affected Markdown event or
upload the PDF as a new version to regenerate missing chunks, then redrive the
staging message if necessary.
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

All six application buckets have versioning enabled. Noncurrent object versions
become eligible for expiration seven days after they become noncurrent; current
objects do not expire under these rules. Deletes create delete markers, and
older versions remain recoverable until the lifecycle policy expires them.
Buckets and queues are retained on stack deletion.

## Transfer and analysis

Writing a staging manifest (`*__manifest.json`) sends an event to the transfer
queue. The transfer Lambda reads that manifest version and ignores the event
if a newer manifest has replaced it. It then queues every listed chunk that
does not already have a current analysis. An analysis is current when the
analyzed object's `source-sha256`, `prompt-sha256` and `model-id` metadata
match the staged chunk, the prompt and the model.

The analyze Lambda reads each chunk by its staged version ID, checks its
SHA-256 hash and sends it to the Global cross-Region inference (CRIS) profile
`global.anthropic.claude-haiku-5-5` through the Bedrock Converse API. Thinking
is disabled, effort is `high`, and the system prompt is cached. Up to 50
analyses run at once by default.

The chunk is sent inside `<document>` tags, and the model is told never to
follow instructions inside them. The prompt asks for reference notes, not a
summary. They must keep names, values and units, defaults, limits, conditions,
negations, procedures, security behavior, tables and code. Generic statements
and descriptions of the source are not allowed. The model works through the
chunk in order and writes one `###` topic block per `#` or `##` section, because
processing packs small sections together. It does not fill in text cut off at
chunk boundaries. It drops PDF conversion artifacts such as running headers,
page numbers and picture placeholders. A chunk with no technical content
produces only `> **Technical Content:** None in this section.`

The chunk's `> Source:` provenance lines are kept unchanged above the model
output. A Markdown code-fence wrapper around the whole response is removed.
Output is rejected and retried if the response was truncated, had a malformed
wrapper, did not start with a `###` heading, used H1 or H2 headings, or left a
code fence unclosed.

Each analysis is written to the analyzed bucket under the staged chunk's key,
with the source hash, source version, prompt hash, model ID and output hash in
its metadata. Very short outputs relative to the input are logged as
`HighCompression` for review.

Claude Haiku 5.5 is available only on the Bedrock Standard tier. Batch
inference and the Flex tier do not support it, so every chunk is an on-demand
request. Caching the system prompt is the main cost saving: every chunk
shares the same system prompt (about 2,500 tokens, above the 512-token cache
minimum), so while the analyze queue keeps requests flowing, each request
after the first in a five-minute window reads it from the cache.

Claude Haiku 5.5 charges more for a request whose prompt exceeds 100,000
tokens. The count includes the system prompt, cache reads, cache writes and
the uncached input. Input and output for that whole request are billed at the
higher rate, even with a cache hit. Claude's tokenizer produces at most one
token per UTF-8 byte, so the analyze Lambda rejects any chunk larger than
100,000 bytes minus the system prompt and 1,000 bytes of request overhead
before calling Bedrock. Processing caps chunks at 15,000 characters, so
normal chunks are far below this limit. If Bedrock's reported prompt tokens
exceed 100,000, the Lambda logs `PricingCliff`. Monitor for both messages.

Global CRIS can route requests from `us-east-2` to any commercial AWS Region.
The Bedrock account must have access to Anthropic models. The analyze role is
granted `bedrock:InvokeModel` on the global inference profile in `us-east-2`,
on the foundation model in `us-east-2` and on the Region-less
`arn:aws:bedrock:::foundation-model/anthropic.claude-haiku-5-5`. Bedrock
evaluates that last resource with `aws:RequestedRegion` set to `unspecified`.
A Region-restricting SCP must therefore allow it, for example by exempting
requests that have a `bedrock:InferenceProfileArn`.

### Verification

After the last chunk of a run is analyzed, the verify Lambda checks every chunk
in the current staging manifest. It holds the PDF's coordination lock while it
does so. Verification succeeds only when each chunk has a current analysis. It
then writes `My guide/source-<source-key-hash>__manifest.json` to the analyzed
bucket and deletes obsolete analyses for that PDF only. The analyzed manifest
lists each chunk's key, analyzed version ID, source SHA-256 and output SHA-256,
plus the staging manifest version, model ID and prompt hash. If any chunk is
missing, verification logs `AnalysisIncomplete` and does not write a manifest.

### Running manually

Both Lambdas accept the PDF's source key. This resumes from the staging bucket
without downloading or reprocessing the PDF:

```sh
aws lambda invoke --region us-east-2 \
  --function-name <TransferFunctionName> \
  --cli-binary-format raw-in-base64-out \
  --payload '{"source_key": "guides/My guide.pdf"}' /dev/stdout

aws lambda invoke --region us-east-2 \
  --function-name <VerifyFunctionName> \
  --cli-binary-format raw-in-base64-out \
  --payload '{"source_key": "guides/My guide.pdf"}' /dev/stdout
```

Transfer queues only chunks that are missing or out of date, then verifies.
Changing the prompt or the model makes every existing analysis out of date. A
manual transfer started while an automatic run for the same PDF is still in
progress may analyze some chunks twice. The function names are stack outputs.

Throttling and temporary Bedrock errors return the message to the queue after
60 seconds, for up to 60 attempts. Other analysis failures, such as truncated
or invalid output, get 3 attempts so failing model calls do not keep billing.
When either limit is reached, the analyze Lambda moves the message to the
analyze dead-letter queue and counts the chunk as settled. The run therefore
still finishes, and verification logs `AnalysisIncomplete` with the missing
chunk keys. The queue's own limit of 100 receives is only a backstop.

Chunks are never lost: the staging manifest and the analyzed bucket are the
record of what is done, and the queues only carry work. After fixing the cause,
run the transfer Lambda manually; it re-queues only missing or outdated
chunks. Monitor the transfer, analyze and verify dead-letter queues.

To stop all analysis immediately, disable the analyze queue trigger. Messages
wait in the queue until it is re-enabled:

```sh
FUNCTION=$(aws cloudformation describe-stack-resources --region us-east-2 \
  --stack-name AlexandriaTransferStack \
  --query "StackResources[?starts_with(LogicalResourceId,'AnalyzeFunction') && ResourceType=='AWS::Lambda::Function'].PhysicalResourceId" \
  --output text)
UUID=$(aws lambda list-event-source-mappings --region us-east-2 \
  --function-name "$FUNCTION" --query "EventSourceMappings[0].UUID" --output text)
aws lambda update-event-source-mapping --region us-east-2 --uuid "$UUID" --no-enabled
```

Use `--enabled` to resume. To discard the queued work instead, purge the
analyze queue with `aws sqs purge-queue`, then run the transfer Lambda again
later.

### Concurrency

Set the number of concurrent analyses at deploy time; the default is 50:

```sh
cdk deploy AlexandriaTransferStack -c analyzeConcurrency=100
```

The practical limit is the account's Bedrock quota
`Cross-Region InvokeModel tokens per minute` for Claude Haiku 5.5 in
`us-east-2`, shown in Service Quotas. Each request counts its input, cache
writes and output against that quota, and each output token counts about five
times. A typical chunk uses about 20,000 quota tokens, and a request takes
about 30 seconds. Each concurrent analysis therefore needs about 40,000 tokens
per minute. Divide the quota by 40,000 to find a concurrency that will not be
throttled. A higher setting is safe, because throttled chunks wait and retry,
but it adds little speed. Lambda concurrency counts against the account's
concurrent-executions limit, which defaults to 1,000.

Each PDF processing function has a 15-minute timeout, 4 GiB memory and 10 GiB
temporary storage. Large or complex PDFs can still exceed these limits,
including during splitting or conversion of a 50-page part. Image-only PDFs
may not yield usable text; empty extraction fails rather than producing an empty
chunk. Partial results may already be present when a stage fails; the presence
of a chunk alone does not prove that the entire PDF finished. Temporary working
files are cleaned after each operation and after an interrupted invocation when
processing resumes.

Each processing and staging queue retains messages for 14 days, uses a 90-minute
visibility timeout, and sends messages to its own dead-letter queue after 100
unsuccessful receives. Processing messages encountering a busy lease or a split
that is still being initialized retry after 30 seconds using SQS partial batch
failures, rather than waiting for the normal 90-minute visibility timeout. Other processing
errors still fail the invocation and use the normal visibility timeout.
Monitor all queues, dead-letter queues and Lambda logs;
there are no configured alerts. Investigate and redrive failed messages before
they expire.

Downloads, split initialization, output publication and staging use a per-source
DynamoDB lease. Splitting also uses a separate per-source split lease to exclude
duplicate splitters, but releases the source lease while downloading and preparing
parts, acquiring it again for each upload and the final ready transition.
The splitter waits up to 30 seconds for the source lease at these transitions;
if it remains busy, the split message is deferred and the next attempt starts
a new generation.
Completion is checked both by the last chunk and by the splitter, so parts that
finish chunking before the splitter marks the generation ready cannot cause premature or missed
staging. Conversion uses a separate per-part lease during extraction, then briefly
acquires the source lease and rechecks the current generation before publishing.
Markdown chunk preparation also runs outside the source lease; chunk publication
and completion markers remain protected. Replacements do not wait for old
extractions, and old results cannot be published after cleanup.
A busy lease causes the message or download to retry. An interrupted
invocation can hold a new lease for up to 16 minutes (existing leases from an older
deployment can still last 31 minutes). The download function clears
old outputs before downloading; the splitter also clears old outputs before
processing a new version, including a manual upload. Old queued events are
ignored. A failed download leaves the prior staging manifest intact, while the
old PDF version is blocked from being processed as a new update.

Run local tests with `python -m unittest discover -s tests`.
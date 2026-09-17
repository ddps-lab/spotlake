# Monthly citation refresh

EventBridge Rule `spotlake-monthly-jobs` runs on the **first day of each month at
00:00 UTC (09:00 Asia/Seoul)**. It independently invokes citation Lambda
`spotlake-monthly-citations` and submits the Google Drive upload Batch job to
`montly_share_raw_dataset_generator_queue`, using the latest active revision of
`monthly_drive_dataset_uploader_definition`. Neither target waits for the other.
The Batch job definition supplies its existing retry/timeout/container settings;
target delivery keeps the previous 185 attempts / 2-hour event-age limits.

The old Schedulers `monthly-drive-dataset-uploader-cron` and
`spotlake-monthly-citations-cron` are retained **DISABLED**, with their original
targets preserved. Do not enable them while the shared Rule is enabled, or jobs
can be submitted twice. The citation Scheduler's disabled state is managed by
CloudFormation; a post-deployment migration script verifies both Rule targets
before disabling the separately managed Drive Scheduler. It refuses unexpected
legacy target overrides rather than silently losing them.

The Python Lambda queries Semantic Scholar for the
IISWC paper, its arXiv version, and the WWW demo. Crossref supplies missing venue
names when available. Google Scholar is a link to the full citation list, not a
scraping source.

The output is `s3://spotlake-public-daily/citations/citations.json`, served through
the existing data distribution at
`https://d2krkjqajp4l0e.cloudfront.net/citations/citations.json`. This is separate
from the website deployment bucket: normal frontend `s3 sync --delete` cannot
remove the snapshot. The existing public-daily producer writes its own named
objects and does not delete this prefix.

## Data and failure behavior

- `schemaVersion`: currently 1.
- `updatedAt`: UTC time of the last successful citation refresh, including checks
  with no new papers. It is not the frontend deployment time or Google Scholar's
  indexing time. Crossref is optional enrichment; failure there does not invalidate
  an otherwise complete citation lookup.
- `source`: `Semantic Scholar + Crossref`.
- `papers`: title, authors, venue, year, and links, matching the frontend's publication
  format. Existing entries and manually refined metadata are preserved. New entries
  are deduplicated by normalized DOI/title. Curated lab papers and known lab-author
  names are excluded from the external list.

Every Semantic Scholar source/page must finish before writing. Rate limiting,
invalid responses, empty aggregate results, and exhausted time budgets leave the
stored list and date unchanged. Requests use bounded backoff and a total 240-second
API budget inside a 300-second function. EventBridge delivery and Lambda execution
also have bounded retries. No per-visitor external API calls, GitHub write token,
proxy, CAPTCHA bypass, VPC, NAT gateway, or container build is involved.

The function can read/write only the citation object and write to its own log
streams. Conditional S3 writes prevent an in-flight refresh from overwriting a
concurrent repair. CloudFront may serve the previous snapshot for up to one hour.
Logs are retained for 30 days; inspect failed invocations in the Lambda/CloudWatch
console. No Slack/email notifications are configured.

## Deployment

`.github/workflows/monthly-citation-deploy.yml` runs on relevant changes merged to
`main`, or a manual dispatch **on main**. It uses the repository's existing
`SPOTRANK_ACCESS_KEY_ID` / `SPOTRANK_SECRET_ACCESS_KEY` deployment secrets. Their
principal needs CloudFormation, Lambda, EventBridge, IAM role management/PassRole,
CloudWatch Logs and Scheduler GetSchedule/UpdateSchedule permissions; the deployed
Lambda role is much narrower. Deployment fails visibly if those CI permissions are insufficient.

The workflow:

1. Runs unit tests and reads curated lab identities from `publications.yaml`.
2. Renders a small CloudFormation template with inline Python code and lab identities.
3. Deploys the `spotlake-monthly-citations` CloudFormation stack, Lambda, shared
   Rule, Lambda resource permission, and Batch submission role. The latter can
   submit only this queue/job-definition family and trusts only this Rule. The
   stack retains the old citation Scheduler in its disabled state.
4. Runs `disable_legacy_schedules.py`: verifies the enabled monthly Rule and both
   expected targets, then disables the legacy Schedulers without changing their
   targets. The script is idempotent and does not run either job. No new bucket,
   Batch queue, job definition, container image, or build host is created.

The shared Rule and Batch event role belong to this CloudFormation stack; deleting
it would stop both monthly triggers. The legacy schedules are retained for an
explicit rollback: disable the shared Rule first, then enable both legacy schedules.
Routine deployments keep only the Rule enabled.

The production S3 snapshot was initialized and successfully refreshed on September
17, 2026. It is the only maintained citation list. There is no GitHub fallback,
bundled citation data, or deployment-time seed step. The updater reads the existing
S3 snapshot to preserve previous entries and curated metadata; a missing object
fails visibly instead of silently replacing the history with an empty list. When
recovering or moving to another bucket, restore the existing snapshot first.

Code deployment does not write the snapshot, change its date, or invoke the
external APIs. A manual invocation can verify AWS execution:

```bash
aws lambda invoke --function-name spotlake-monthly-citations \
  --region us-west-2 --profile spotrank_jaeil --cli-read-timeout 310 \
  /tmp/spotlake-citation-result.json
```

Check both the CLI response's `FunctionError` field and the result/logs: HTTP 200
from the invocation API alone does not prove handler success. No production
resource is created by rendering or validating the template.

## Local checks

```bash
python3 -m unittest discover -s utility/monthly_citation_updater/tests -v
cd frontend
npm ci
npm run build
```

For an optional API dry-run, prepare lab identities and download the current AWS
snapshot into a temporary directory, then run the Python collector without an
`--output` argument:

```bash
node frontend/scripts/prepare-citations.mjs /tmp/spotlake-citation-check
aws s3api get-object --bucket spotlake-public-daily --key citations/citations.json \
  --region us-west-2 --profile spotrank_jaeil /tmp/spotlake-citation-check/current.json
python3 utility/monthly_citation_updater/lambda_function.py \
  --seed /tmp/spotlake-citation-check/current.json \
  --lab-papers /tmp/spotlake-citation-check/lab-papers.json
```

This calls real APIs and prints a summary without writing a citation file or
uploading to AWS. It does not modify `publications.yaml`. Do not commit downloaded
citation snapshots. Routine monthly refresh requires no local run or commit.

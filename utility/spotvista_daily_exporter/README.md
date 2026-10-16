# SpotVista daily exporter

Lambda `spotvista-daily-exporter` runs **every day at 01:00 UTC** and publishes, for
each reference date, one JSON with the availability score of every spot instance. The
static SpotVista site downloads that file, filters it by the selected instance types and
regions, and applies only the cost score and weight. It replaces SpotVista's query Lambda
(`POST /api/query`).

## Data flow

Input is the TITANS change-point data in `s3://titans-spotlake-data`:

- past months: `aws/YYYY-MM_AP.parquet` (state at the start of the month) and
  `aws/YYYY-MM.parquet` (all change points of the month);
- current month: the same anchor plus every file listed in
  `parquet_warm/aws/m8/YYYY/MM/manifest.json` (Warm levels and unmerged Hot files).
  If the manifest has not processed the day's 23:50 tick yet, the day is skipped
  and the next run retries it.

For each day the function rebuilds the 10-minute grid per (InstanceType, Region, AZ):
tick 0 is the last row at or before 00:00, ticks 1–143 take that day's change points,
and gaps carry the previous value forward. A row at the next day's 00:00 belongs to
the next day. `Ceased` rows (recorded with zero values) mark the entry as absent until
its next normal row. Months recorded before the `Ceased` column existed cannot show
removals, so the last value carries on.

Each day's aggregates are combined with the six days before it into the
availability score of the original query module (`calculate_score.py`): the area,
standard deviation and slope of the 7-day T3 series only need the count, sum, sum of
squares, `Σ x·y`, first and last value of each day.

Output goes to `s3://spotlake-public-daily/spotvista/aws/`, served at
`https://d2krkjqajp4l0e.cloudfront.net/spotvista/aws/`:

- `scores/YYYY-MM-DD.json` (reference date R, window R-7 … R-1): every candidate whose
  latest value in the window has `SPS > 0` and `SpotPrice > 0`, as
  `rows[InstanceType][Region] = [[AZ, SPS, T3, SpotPrice, AvailabilityScore], …]`.
  `SPS`, `T3` and `SpotPrice` are the latest values in the window; the score is rounded to
  4 decimals. About 50,000 rows, 1.9 MB (about 0.5 MB compressed by CloudFront).
  The cost score depends on which candidates are selected, so the browser computes it.
- `index.json`: first and last reference date, and the InstanceType → Region list of the
  latest reference date for the selectors.
- `agg/YYYY-MM-DD.json`: that day's six T3 aggregates (`n`, `sum`, `sumsq`, `sum_iy`,
  `first`, `last`) and last `SPS` and positive `SpotPrice` per (InstanceType, Region, AZ),
  column-wise. Intermediate result read back to build the next seven reference dates.
  The 10-minute curve itself is not published.
- `agg/YYYY-MM-DD.done`: written after the scores and `index.json`; a day without it is
  rebuilt by the next run.

When a day is (re)built, every reference date whose window contains it and whose
previous day already exists is rewritten.

T3 exists from 2025-02-15, so that is the first day aggregated and 2025-02-22 the first
reference date scored.

## Invocation

- `{}` (daily rule): yesterday, plus any of the last 7 days still missing `.done`.
- `{"date": "YYYY-MM-DD"}`: rebuild one day.
- `{"month": "YYYY-MM"}`: build every day of a month (backfill). Run months one by one,
  oldest first, so each window finds the days before it:

```bash
aws lambda invoke --function-name spotvista-daily-exporter \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 900 \
  --payload '{"month":"2025-02"}' /dev/stdout
```

## Deployment

`.github/workflows/spotvista-daily-exporter-deploy.yml` runs on relevant changes merged
to `main`, or a manual dispatch on main. Like the monthly citation updater it renders a
self-contained CloudFormation template (inline code, no container or artifact bucket)
and deploys stack `spotvista-daily-exporter` with the `SPOTRANK_*` secrets.

The function role can read only the three TITANS prefixes above and write only under
`spotlake-public-daily/spotvista/aws/`. Reserved concurrency is 1 so runs never overlap.

## Dependencies layer

TITANS parquet is zstd-compressed and is read with DuckDB (the AWS SDK for pandas managed
layer ships a pyarrow without zstd). DuckDB and numpy live in the layer
`spotvista-exporter-deps`, published once in this account and pinned by version in
`render_template.py`. To change versions, build and publish a new version, then bump the
pin and the versions in the workflow's test step:

```bash
pip install --platform manylinux_2_28_aarch64 --platform manylinux_2_17_aarch64 \
  --only-binary=:all: --python-version 3.13 --implementation cp \
  --target layer/python duckdb==1.5.6 numpy==2.5.4
(cd layer && zip -qr9 ../layer.zip python -x '*/__pycache__/*')   # about 34 MB
aws lambda publish-layer-version --layer-name spotvista-exporter-deps \
  --compatible-runtimes python3.13 --compatible-architectures arm64 \
  --zip-file fileb://layer.zip
```

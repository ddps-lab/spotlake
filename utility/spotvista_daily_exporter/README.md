# SpotVista daily exporter

Lambda `spotvista-daily-exporter` runs **every day at 01:00 UTC** and publishes the
daily T3 aggregates that the static SpotVista site uses to score spot instances.
It replaces SpotVista's query Lambda (`POST /api/query`).

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

Output goes to `s3://spotlake-public-daily/spotvista/aws/`, served at
`https://d2krkjqajp4l0e.cloudfront.net/spotvista/aws/`:

- `daily/YYYY-MM-DD/{InstanceType}.json`: one row per AZ with six T3 aggregates
  (`n`, `sum`, `sumsq`, `sum_iy`, `first`, `last`) and the day's last values
  (`Time`, `SPS`, `T3`, last positive `SpotPrice`). The 10-minute curve itself is not
  published; SpotVista combines seven days of aggregates into the same area, standard
  deviation and slope that the original query module computed.
- `daily/YYYY-MM-DD/_done`: written last; a day without it is rebuilt by the next run.
- `index.json`: available date range and the InstanceType → Region list.

T3 exists from 2025-02-15, so that is the first day produced.

## Invocation

- `{}` (daily rule): yesterday, plus any of the last 7 days still missing `_done`.
- `{"date": "YYYY-MM-DD"}`: rebuild one day.
- `{"month": "YYYY-MM"}`: build every day of a month (backfill). Run months one by one:

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

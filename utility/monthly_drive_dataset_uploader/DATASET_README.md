# SpotLake Public Dataset

Historical Spot instance prices and availability indicators collected by the DDPS Lab at Hanyang University. The public window covers calendar months 6 to 18 months before the current month, inclusive (13 months).

Each provider has one ZIP per month: `<provider>/<provider>-YYYY-MM.zip`. Each ZIP contains daily files named `<provider>-YYYY-MM/<provider>-YYYY-MM-DD.parquet`. The intended collection intervals are 10 minutes for AWS/Azure and 1 hour for GCP; actual coverage can have gaps.

These are normalized datasets derived from collected CSV snapshots. Column names and types may be standardized, missing columns filled with null, and duplicate rows with the same entity key and timestamp removed. `Time` comes from the source file timestamp. The export does not resample the observations to a coarser time interval.

## AWS columns

Entity key: `InstanceType`, `Region`, `AZ`.

| Column | Meaning and values |
|---|---|
| `Time` | Snapshot timestamp in UTC, stored without a timezone suffix. |
| `InstanceType` | EC2 instance type, such as `m5.large`. |
| `Region` | AWS region code, such as `us-west-2`. |
| `AZ` | Availability Zone ID, such as `usw2-az1`. |
| `SPS` | [AWS Spot placement score](https://docs.aws.amazon.com/cli/latest/reference/ec2/get-spot-placement-scores.html). Scores range from 1 to 10; higher means a higher estimated likelihood of obtaining the requested capacity. Values above 3 are valid. This is not a percentage or a capacity guarantee. |
| `T3` | Collector-maintained target instance count associated with a placement score of at least 3. Derived from tested target counts and retained collector state, not a guaranteed maximum launchable capacity. |
| `T2` | Same interpretation as `T3`, with a score threshold of at least 2. |
| `IF` | Encoded interruption-frequency category; see the mapping below. Higher values mean a lower interruption-frequency category. |
| `OndemandPrice` | Collected on-demand price in USD per instance-hour. |
| `SpotPrice` | Collected Spot price in USD per instance-hour. |
| `Savings` | Discount relative to on-demand pricing, in percent: `(1 - SpotPrice / OndemandPrice) * 100`, when both prices are valid. Values may be rounded. |

`T2`/`T3` preserve historical collector outputs, including any historical collector anomalies; they are not recomputed during this export.

## Azure columns

Entity key: `InstanceTier`, `InstanceType`, `Region`, `AZ`.

| Column | Meaning and values |
|---|---|
| `Time` | Snapshot timestamp in UTC, stored without a timezone suffix. |
| `InstanceTier` | VM SKU prefix/tier, such as `Standard`. |
| `InstanceType` | VM size portion of the SKU, such as `D2s_v3`. |
| `Region` | Azure region label as recorded by the collector. Historical labels may differ from API region codes. |
| `AZ` | Availability Zone identifier, such as `1`, `2`, or `3`. `Single` is a collector fallback when the score response has no zone identifier; `N/A` is an unspecified/unavailable marker. Neither denotes an additional physical zone. |
| `Score` | [Azure Spot placement score](https://learn.microsoft.com/en-us/azure/virtual-machine-scale-sets/spot-placement-score). Historical labels include `High`, `Medium`, and `Low`; the collector's numeric equivalents are 3, 2, and 1, respectively. Higher means a higher estimated likelihood of obtaining the requested VM count. Historical status labels such as `RestrictedSkuNotAvailable` indicate a restriction/unavailability, not a numeric score. |
| `DesiredCount` | Number of VMs requested in the placement-score query. It is not the number of running or allocated VMs. |
| `T3` | Collector-maintained target VM count associated with a score of at least 3 (`High`), derived from tested counts and retained collector state. It is not a capacity guarantee. |
| `T2` | Same interpretation as `T3`, with a score threshold of at least 2 (`Medium`). |
| `IF` | Encoded eviction-rate category; see the mapping below. Higher values mean a lower eviction-rate category. |
| `OndemandPrice` | Collected on-demand price in USD per VM-hour. |
| `SpotPrice` | Collected Spot price in USD per VM-hour. |
| `Savings` | Discount relative to on-demand pricing, in percent: `(1 - SpotPrice / OndemandPrice) * 100`, when both prices are valid. Values may be rounded. |

`Score` can be numeric or textual in historical archives. Newly generated archives store it as text to preserve both labels and numeric scores, including `"1"`, `"2"`, and `"3"`. Zone coverage and labels also vary over time; an unspecified zone is not a one-to-one substitute for a specific zone.

The Azure collector changed its score representation within this dataset. The mapping is `High` = `3`, `Medium` = `2`, and `Low` = `1`; this is an Azure history change, not a difference between AWS and Azure. The export retains the original label or numeric value rather than remapping it.

| Source snapshot time (UTC) | Observed Azure `Score` representation |
|---|---|
| 2025-12-12 09:40 | Text labels (`High`, `Medium`, `Low`) and status labels were still present. |
| 2025-12-12 10:30–16:50, sampled snapshots | All `Score` values were null during this transition; these are missing observations, not `Low` or zero scores. |
| 2025-12-12 17:00 | Numeric scores `3`, `2`, `1`, with `0` for statuses outside that mapping; some nulls remained. |
| 2026-01-03 11:30 → 11:40 | The unavailable-status sentinel changed from `0` to `-1`; valid scores remained `1`, `2`, `3`. |

These boundaries were checked against source snapshots and collector history; they are not an exhaustive audit of every historical interval. Historical `0` and later `-1` are unavailable-status markers, not valid scores below `Low`. A numeric sentinel does not preserve the original specific status (for example, `RestrictedSkuNotAvailable` versus `DataNotFoundOrStale`).


## GCP columns

Entity key: `InstanceType`, `Region`.

| Column | Meaning and values |
|---|---|
| `Time` | Snapshot timestamp in UTC, stored without a timezone suffix. |
| `InstanceType` | Compute Engine machine type. |
| `Region` | GCP region identifier. |
| `OndemandPrice` | Collected/computed on-demand price in USD per VM-hour. |
| `SpotPrice` | Collected/computed Spot price in USD per VM-hour. |
| `Savings` | Discount relative to on-demand pricing, in percent: `(1 - SpotPrice / OndemandPrice) * 100`, when both prices are valid. Values may be rounded. |

The GCP dataset does not include `AZ`, placement scores, `T2`/`T3`, or `IF`.

## Interruption-frequency encoding

`IF` is an ordinal encoding, **not a percentage**. The collectors map the provider's published categories as follows; the category labels below retain the providers' different boundary notation.

| Stored `IF` | AWS interruption-frequency category | Azure eviction-rate category |
|---|---|---|
| `3.0` | `<5%` | `0-5%` |
| `2.5` | `5-10%` | `5-10%` |
| `2.0` | `10-15%` | `10-15%` |
| `1.5` | `15-20%` | `15-20%` |
| `1.0` | `>20%` | `20%+` |

## Missing values and stored types

| Value or condition | Interpretation |
|---|---|
| `null` | No value is stored. This includes columns absent from older source schemas, such as historical Azure `T2`/`T3`. The CSV converter writes nulls as empty fields. |
| `-1`, `-1.0`, or textual `"-1"` | Collector sentinel for an unavailable, unsupported, or invalid numeric observation. It must not be treated as a valid price, count, or interruption category. The sentinel alone does not identify the reason. |
| `N/A` | Historical text marker for unspecified/unavailable information, including Azure zone/score fields. |
| `0` in historical Azure `Score` | Unavailable/unmapped status under the older collector mapping; later replaced by `-1`. See the transition notes above. |
| `0` in `T2`/`T3` | No qualifying positive target count is recorded. This can also be a collector default; it does not prove that physical capacity is zero. |
| Missing row or timestamp | No record is provided for that entity/time. Missing intervals are not filled with synthetic rows. |

Other zero values must be interpreted according to the column; do not treat every zero as missing.

| Columns | Stored type |
|---|---|
| AWS `SPS`, `T2`, `T3`; Azure `DesiredCount`, `T2`, `T3` | Nullable 64-bit integer (`Int64`). Missing values remain null; unavailable `-1` remains `-1`. |
| `IF`, `OndemandPrice`, `SpotPrice`, `Savings` | Floating point. Fractional values are preserved. Some historical daily files may infer an integer type when all values are integral (for example, `IF = -1` throughout). |
| Azure `Score` | New exports use text, preserving both historical labels and numeric representations. Older archives may retain numeric types. |

Integer conversion checks that no fractional values are lost. The CSV converter handles historical integer/float and numeric/text type differences when combining files; that can make an integer value appear as `3.0` in a combined CSV. Extra source columns may be retained. Internal `id` and `SPS_Update_Time` columns are not included; the converter does not reconstruct them.

## Convert Parquet to CSV

Install Python 3.10 or later and Polars (tested with 1.44.2):

```bash
python -m pip install 'polars>=1.44.2,<2'
```

### Single CSV (default)

```bash
python parquet_to_csv.py aws-2025-03/aws-2025-03-15.parquet -o aws-2025-03-15.csv.gz
```

One daily Parquet produces one daily CSV containing all observed timestamps. Passing a monthly ZIP combines its daily files into one monthly CSV. Multiple Parquet/ZIP paths and quoted wildcard patterns are supported. When combining files, compatible numeric types are promoted, mixed numeric/text values are preserved as text, and missing columns become empty fields. Combine files from the same provider. Add `--sort-by-time` for ascending timestamp order. Use `.gz` for gzip compression or `.csv` for uncompressed output.

### CSV per collection timestamp

```bash
python parquet_to_csv.py aws-2025-03.zip --mode tick -o snapshots
```

This writes `snapshots/aws-2025-03/DD/HH-MM-SS.csv.gz`, following the source archive's day/time organization. Each file contains only the rows with that exact `Time`. AWS/Azure usually have 10-minute intervals and GCP usually has hourly intervals; actual observed timestamps are used, without rounding, resampling, or filling gaps. Standard SpotLake filenames identify the provider; otherwise add `--provider aws`, `--provider azure`, or `--provider gcp`.

The CSV columns remain those of the normalized Parquet dataset: renamed columns keep their current names, and deleted columns are not restored. This is a time-partitioned export, not a byte-for-byte reconstruction of the source CSV. Historical normalization and duplicate removal cannot be reversed by this converter. Input files that overlap at the same provider/timestamp are rejected to prevent accidental overwriting.

### Storage and checks

The output file or directory must not already exist. A failed conversion removes its partial output. ZIP input is extracted to temporary storage and removed afterward. Allow disk space for the extracted Parquet files and CSV output; CSV can be much larger than Parquet, and timestamp partitioning repeats CSV headers and reduces compression opportunities.

Both modes process data in batches/streams. Sorting a combined CSV can increase runtime and memory or temporary-disk use. Timestamps are written as `YYYY-MM-DD HH:MM:SS` in UTC, and nulls as empty CSV fields. Tick mode requires non-null, whole-second timestamps. Run the basic installation check with:

```bash
python parquet_to_csv.py --selftest
```

## Project and other data requests

- [SpotLake — DDPS Lab, Hanyang University](https://spotlake.ddps.cloud)
- [Request another period or dataset](https://forms.gle/GjhsuybJkUt5LMFc7)

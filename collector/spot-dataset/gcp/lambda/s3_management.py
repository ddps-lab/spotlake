import gzip
import io
import json
import os
import time
from datetime import datetime

import boto3
import polars as pl
from botocore.config import Config
from botocore.exceptions import ClientError

from collector_core import SNAPSHOT_SCHEMA
from const_config import GcpCollector, Storage
from runtime_config import load_runtime_config
from utility import slack_msg_sender

session = boto3.session.Session(region_name="us-west-2")
write_client = session.client(
    "timestream-write",
    config=Config(read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}),
)

STORAGE_CONST = Storage()
LOCAL_PATH = GcpCollector().LOCAL_PATH


def _set_public_read(s3_resource, bucket_name, key):
    runtime_config = load_runtime_config()
    if not runtime_config.public_read_enabled:
        return
    object_acl = s3_resource.ObjectAcl(bucket_name, key)
    object_acl.put(ACL="public-read")


def _empty_snapshot_df() -> pl.DataFrame:
    return pl.DataFrame(schema=SNAPSHOT_SCHEMA)


def _normalize_snapshot_df(df: pl.DataFrame) -> pl.DataFrame:
    if "time" in df.columns and "Time" not in df.columns:
        df = df.rename({"time": "Time"})
    if "id" in df.columns:
        df = df.drop("id")

    expressions = []
    for column, dtype in SNAPSHOT_SCHEMA.items():
        if column in df.columns:
            expressions.append(pl.col(column).cast(dtype, strict=False))
        else:
            expressions.append(pl.lit(None, dtype=dtype).alias(column))
    return df.with_columns(expressions).select(list(SNAPSHOT_SCHEMA.keys())).drop_nulls()


def submit_batch(records, counter, recursive, *, database_name, table_name):
    if recursive == 10:
        return
    try:
        write_client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            Records=records,
            CommonAttributes={},
        )
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            slack_msg_sender.send_slack_message({rr["Reason"]})
            print(rr["Reason"])
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(
            re_records,
            counter,
            recursive + 1,
            database_name=database_name,
            table_name=table_name,
        )
    except Exception as err:
        slack_msg_sender.send_slack_message(err)
        print(err)
        raise


def upload_timestream(data: pl.DataFrame, timestamp):
    runtime_config = load_runtime_config()
    if not runtime_config.timestream_enabled:
        print("[GCP Collector] GCP_TIMESTREAM_ENABLED=0; skipping Timestream upload.")
        return True

    print(data.height)

    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), "%Y-%m-%d %H:%M")
    time_value = time.mktime(time_value)
    time_value = str(int(round(time_value * 1000)))

    records = []
    counter = 0
    for row in data.iter_rows(named=True):
        dimensions = []
        for column in ["InstanceType", "Region", "Ceased"]:
            if column in row and row[column] is not None:
                dimensions.append({"Name": column, "Value": str(row[column])})

        submit_data = {
            "Dimensions": dimensions,
            "MeasureName": "gcp_values",
            "MeasureValues": [],
            "MeasureValueType": "MULTI",
            "Time": time_value,
        }
        for column, types in [("OnDemand Price", "DOUBLE"), ("Spot Price", "DOUBLE")]:
            submit_data["MeasureValues"].append({"Name": column, "Value": str(row[column]), "Type": types})
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(
                records,
                counter,
                0,
                database_name=runtime_config.write_database_name,
                table_name=runtime_config.write_table_name,
            )
            records = []

    if records:
        submit_batch(
            records,
            counter,
            0,
            database_name=runtime_config.write_database_name,
            table_name=runtime_config.write_table_name,
        )

    print(f"end : {counter}")
    return True


def load_latest_state() -> pl.DataFrame:
    runtime_config = load_runtime_config()
    s3 = session.resource("s3")
    try:
        response = s3.Object(
            runtime_config.read_bucket_name,
            runtime_config.latest_read_path,
        ).get()
    except ClientError as e:
        if e.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return _empty_snapshot_df()
        raise

    payload = json.loads(response["Body"].read())
    if not payload:
        return _empty_snapshot_df()
    return _normalize_snapshot_df(pl.from_dicts(payload))


def update_latest(data: pl.DataFrame, timestamp):
    runtime_config = load_runtime_config()
    filename = "latest_gcp.json"
    latest_df = data.with_columns(
        [
            pl.when(pl.col(column) == 0)
            .then(pl.lit(-1.0))
            .otherwise(pl.col(column))
            .alias(column)
            for column in ["OnDemand Price", "Spot Price", "Savings"]
            if column in data.columns
        ]
    )
    latest_df = (
        latest_df
        .with_row_index("id", offset=1)
        .select(["id", "InstanceType", "Region", "OnDemand Price", "Spot Price", "Savings"])
        .with_columns(pl.lit(datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")).alias("time"))
    )

    with open(f"{LOCAL_PATH}/{filename}", "w", encoding="utf-8") as f:
        json.dump(latest_df.to_dicts(), f)

    s3 = boto3.client("s3")
    with open(f"{LOCAL_PATH}/{filename}", "rb") as f:
        s3.upload_fileobj(f, runtime_config.write_bucket_name, runtime_config.latest_write_path)

    s3_resource = boto3.resource("s3")
    _set_public_read(s3_resource, runtime_config.write_bucket_name, runtime_config.latest_write_path)
    return True


def update_query_selector(changed_df: pl.DataFrame):
    runtime_config = load_runtime_config()
    if not runtime_config.query_selector_enabled:
        print("[GCP Collector] GCP_QUERY_SELECTOR_ENABLED=0; skipping query selector update.")
        return True

    filename = "query-selector-gcp.json"
    s3 = session.resource("s3")
    try:
        existing = json.loads(
            s3.Object(
                runtime_config.query_selector_read_bucket_name,
                runtime_config.query_selector_read_path,
            ).get()["Body"].read()
        )
        query_selector_gcp = pl.from_dicts(existing) if existing else pl.DataFrame(
            schema={"InstanceType": pl.Utf8, "Region": pl.Utf8}
        )
        query_selector_gcp = pl.concat(
            [
                query_selector_gcp.select(["InstanceType", "Region"]),
                changed_df.select(["InstanceType", "Region"]),
            ],
            how="diagonal_relaxed",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] not in {"404", "NoSuchKey"}:
            raise
        query_selector_gcp = changed_df.select(["InstanceType", "Region"])

    query_selector_gcp = (
        query_selector_gcp
        .drop_nulls()
        .unique(subset=["InstanceType", "Region"])
        .sort(["InstanceType", "Region"])
    )
    with open(f"/tmp/{filename}", "w", encoding="utf-8") as f:
        json.dump(query_selector_gcp.to_dicts(), f)

    s3_client = session.client("s3")
    with open(f"/tmp/{filename}", "rb") as f:
        s3_client.upload_fileobj(
            f,
            runtime_config.query_selector_write_bucket_name,
            runtime_config.query_selector_write_path,
        )
    s3_resource = session.resource("s3")
    _set_public_read(
        s3_resource,
        runtime_config.query_selector_write_bucket_name,
        runtime_config.query_selector_write_path,
    )
    return True


def save_raw(data: pl.DataFrame, timestamp):
    runtime_config = load_runtime_config()
    save_filename = f"{LOCAL_PATH}/spotlake_{timestamp}.csv.gz"
    raw_df = (
        data
        .with_columns(
            [
                pl.struct(["OnDemand Price", "Spot Price"])
                .map_elements(
                    lambda row: float(round((row["OnDemand Price"] - row["Spot Price"]) / row["OnDemand Price"] * 100)),
                    return_dtype=pl.Float64,
                )
                .alias("Savings"),
                pl.lit(datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")).alias("Time"),
            ]
        )
        .select(["Time", "InstanceType", "Region", "OnDemand Price", "Spot Price", "Savings"])
    )

    csv_bytes = raw_df.write_csv().encode("utf-8")
    with gzip.open(save_filename, "wb") as f:
        f.write(csv_bytes)

    s3 = boto3.client("s3")
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")
    with open(save_filename, "rb") as f:
        s3.upload_fileobj(
            f,
            runtime_config.write_bucket_name,
            f"{runtime_config.raw_prefix}/{s3_dir_name}/{s3_obj_name}.csv.gz",
        )

    for filename in os.listdir(f"{LOCAL_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{LOCAL_PATH}/{filename}")
    return True


def upload_metadata(filename):
    s3 = boto3.client("s3")
    with open(f"{LOCAL_PATH}/{filename}.json", "rb") as f:
        s3.upload_fileobj(
            f, STORAGE_CONST.BUCKET_NAME, f"gcp_metadata/{filename}.json"
        )


def load_metadata(filename):
    obj_file = f"gcp_metadata/{filename}.json"
    save_file = f"{LOCAL_PATH}/{filename}.json"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(STORAGE_CONST.BUCKET_NAME)
    bucket.download_file(obj_file, save_file)

    with open(save_file, "r", encoding="utf-8") as f:
        return json.load(f)

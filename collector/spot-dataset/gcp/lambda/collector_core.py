from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import polars as pl
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

BILLING_URLS = {
    "v2beta/skus": {
        "base_url": "https://cloudbilling.googleapis.com/v2beta/skus",
        "default_params": {
            "pageSize": "5000",
            "filter": 'service="services/6F81-5844-456A"',
        },
        "items_key": "skus",
    },
    "v1beta/skus": {
        "base_url": "https://cloudbilling.googleapis.com/v1beta/skus",
        "default_params": {
            "pageSize": "5000",
            "currencyCode": "USD",
        },
        "items_key": "prices",
    },
}

COMPUTE_BASE_URL = "https://compute.googleapis.com/compute/v1"

SKU_SCHEMA = {
    "skuId": pl.Utf8,
    "machineFamily": pl.Utf8,
    "machineModel": pl.Utf8,
    "region": pl.Utf8,
    "priceModel": pl.Utf8,
    "priceResource": pl.Utf8,
    "displayName": pl.Utf8,
}

GPU_SKU_SCHEMA = {
    "skuId": pl.Utf8,
    "gpuType": pl.Utf8,
    "region": pl.Utf8,
    "priceModel": pl.Utf8,
    "priceResource": pl.Utf8,
    "displayName": pl.Utf8,
}

PRICE_SCHEMA = {
    "skuId": pl.Utf8,
    "currencyCode": pl.Utf8,
    "price": pl.Float64,
    "unit": pl.Utf8,
    "unitQuantity": pl.Utf8,
}

MACHINE_TYPE_SCHEMA = {
    "machineFamily": pl.Utf8,
    "machineType": pl.Utf8,
    "region": pl.Utf8,
    "vcpus": pl.Int64,
    "memory": pl.Float64,
    "gpuCount": pl.Int64,
    "gpuType": pl.Utf8,
}

SNAPSHOT_SCHEMA = {
    "Time": pl.Utf8,
    "InstanceType": pl.Utf8,
    "Region": pl.Utf8,
    "OnDemand Price": pl.Float64,
    "Spot Price": pl.Float64,
    "Savings": pl.Float64,
}


def service_account_path(explicit_path: str | None = None) -> str:
    path = explicit_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")
    return path


def load_service_account_info(explicit_path: str | None = None) -> dict:
    with open(service_account_path(explicit_path), "r", encoding="utf-8") as f:
        return json.load(f)


def get_access_token(explicit_path: str | None = None) -> str:
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path(explicit_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    credentials.refresh(Request())
    return credentials.token


def jaccard_similarity(str1: str, str2: list[str]) -> str:
    max_similarity = 0.0
    max_str = str1
    str1_tokened = str1.split("-")
    check = False
    new_str1 = []
    for token in str1_tokened:
        if any(char.isdigit() for char in token):
            check = True
        if check:
            new_str1.append(token)
    new_str1 = "".join(new_str1).upper()
    for candidate in str2:
        set1, set2 = set(new_str1), set(candidate)
        union = set1 | set2
        if not union:
            continue
        similarity = len(set1 & set2) / len(union)
        if similarity > max_similarity and new_str1[:2] == candidate[:2]:
            max_similarity = similarity
            max_str = candidate
    return max_str


def _request_json(url: str, token: str, *, params: dict | None = None, timeout: int = 60) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    max_attempts = int(os.environ.get("GCP_API_MAX_ATTEMPTS", "5"))
    backoff_seconds = 1.0

    for attempt in range(1, max_attempts + 1):
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        if response.ok:
            return response.json()

        if attempt == max_attempts or not _should_retry_response(response):
            response.raise_for_status()

        time.sleep(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2.0, 16.0)

    raise RuntimeError(f"request failed for url={url}")


def _should_retry_response(response: requests.Response) -> bool:
    if response.status_code in {429, 500, 502, 503, 504}:
        return True

    if response.status_code != 403:
        return False

    try:
        payload = response.json()
    except ValueError:
        return False

    errors = payload.get("error", {}).get("errors", [])
    return any(error.get("reason") == "rateLimitExceeded" for error in errors)


def _paginate_items(url: str, token: str, *, params: dict | None = None, items_key: str = "items") -> list[dict]:
    collected: list[dict] = []
    next_page_token: str | None = None
    while True:
        request_params = dict(params or {})
        if next_page_token:
            request_params["pageToken"] = next_page_token
        response = _request_json(url, token, params=request_params)
        collected.extend(response.get(items_key, []))
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            return collected


def fetch_billing_catalog(access_token: str) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    sku_infos: list[dict] = []
    gpu_sku_infos: list[dict] = []
    for response in _paginate_api_responses("v2beta/skus", access_token):
        new_sku_infos, new_gpu_sku_infos = get_sku_infos(response)
        sku_infos.extend(new_sku_infos)
        gpu_sku_infos.extend(new_gpu_sku_infos)

    sku_ids = {sku_info["skuId"] for sku_info in sku_infos}
    gpu_sku_ids = {gpu_sku_info["skuId"] for gpu_sku_info in gpu_sku_infos}

    price_infos: list[dict] = []
    gpu_price_infos: list[dict] = []
    for response in _paginate_api_responses("v1beta/skus", access_token, sku_id="-"):
        new_price_infos, new_gpu_price_infos = get_price_infos(response, sku_ids, gpu_sku_ids)
        price_infos.extend(new_price_infos)
        gpu_price_infos.extend(new_gpu_price_infos)

    return sku_infos, gpu_sku_infos, price_infos, gpu_price_infos


def _paginate_api_responses(version: str, access_token: str, *, sku_id: str | None = None) -> list[dict]:
    config = BILLING_URLS[version]
    params = dict(config["default_params"])
    url = config["base_url"]
    if version == "v1beta/skus":
        url = f"{url}/{sku_id}/prices"
    response = _request_json(url, access_token, params=params)
    responses = [response]
    while "nextPageToken" in response:
        params["pageToken"] = response["nextPageToken"]
        response = _request_json(url, access_token, params=params)
        responses.append(response)
    return responses


def list_regions_and_machine_types_rest(
    gpu_families: list[str],
    *,
    access_token: str,
    project_id: str,
) -> list[dict]:
    regions = _paginate_items(
        f"{COMPUTE_BASE_URL}/projects/{project_id}/regions",
        access_token,
    )
    zones = _paginate_items(
        f"{COMPUTE_BASE_URL}/projects/{project_id}/zones",
        access_token,
    )

    zones_by_region: dict[str, list[str]] = {}
    for zone in zones:
        region_url = zone.get("region", "")
        region_name = region_url.rsplit("/", 1)[-1] if region_url else ""
        zone_name = zone.get("name")
        if not region_name or not zone_name:
            continue
        zones_by_region.setdefault(region_name, []).append(zone_name)

    region_machine_types: list[dict] = []
    finded_region_machine_types: set[tuple[str, str]] = set()
    for region in regions:
        region_name = region.get("name")
        if not region_name:
            continue
        for zone_name in zones_by_region.get(region_name, []):
            machine_types = _paginate_items(
                f"{COMPUTE_BASE_URL}/projects/{project_id}/zones/{zone_name}/machineTypes",
                access_token,
            )
            for machine_type in machine_types:
                machine_type_name = machine_type.get("name")
                if not machine_type_name:
                    continue
                dedup_key = (region_name, machine_type_name)
                if dedup_key in finded_region_machine_types:
                    continue
                finded_region_machine_types.add(dedup_key)

                gpu_count = 0
                gpu_type = None
                accelerators = machine_type.get("accelerators") or []
                if accelerators:
                    accelerator = accelerators[0]
                    gpu_count = int(accelerator.get("guestAcceleratorCount", 0) or 0)
                    accelerator_type = accelerator.get("guestAcceleratorType")
                    if accelerator_type:
                        gpu_type = jaccard_similarity(accelerator_type, gpu_families)

                region_machine_types.append(
                    {
                        "machineFamily": machine_type_name.split("-")[0].upper(),
                        "machineType": machine_type_name,
                        "region": region_name,
                        "vcpus": int(machine_type.get("guestCpus", 0) or 0),
                        "memory": float(machine_type.get("memoryMb", 0) or 0) / 1024.0,
                        "gpuCount": gpu_count,
                        "gpuType": gpu_type,
                    }
                )

    return region_machine_types


def list_regions_and_machine_types_sdk(
    gpu_families: list[str],
    *,
    service_account_file: str,
) -> list[dict]:
    from google.cloud import compute_v1

    client = compute_v1.RegionsClient.from_service_account_file(service_account_file)
    machine_types_client = compute_v1.MachineTypesClient.from_service_account_file(service_account_file)
    zones_client = compute_v1.ZonesClient.from_service_account_file(service_account_file)

    project_id = load_service_account_info(service_account_file)["project_id"]
    zones = list(zones_client.list(project=project_id))
    region_machine_types: list[dict] = []
    finded_region_machine_types: set[tuple[str, str]] = set()

    for region in client.list(project=project_id):
        zone_list = [zone.name for zone in zones if zone.name.startswith(region.name)]
        for zone_name in zone_list:
            for machine_type in machine_types_client.list(project=project_id, zone=zone_name):
                dedup_key = (region.name, machine_type.name)
                if dedup_key in finded_region_machine_types:
                    continue
                finded_region_machine_types.add(dedup_key)

                gpu_count = 0
                gpu_type = None
                if "accelerators" in machine_type:
                    gpu_count = machine_type.accelerators[0].guest_accelerator_count
                    gpu_type = jaccard_similarity(
                        machine_type.accelerators[0].guest_accelerator_type,
                        gpu_families,
                    )

                region_machine_types.append(
                    {
                        "machineFamily": machine_type.name.split("-")[0].upper(),
                        "machineType": machine_type.name,
                        "region": region.name,
                        "vcpus": machine_type.guest_cpus,
                        "memory": machine_type.memory_mb / 1024.0,
                        "gpuCount": gpu_count,
                        "gpuType": gpu_type,
                    }
                )

    return region_machine_types


def get_sku_infos(response: dict) -> tuple[list[dict], list[dict]]:
    skus = response["skus"]
    sku_infos = []
    gpu_sku_infos = []
    for sku in skus:
        info_type = None
        categories = sku["productTaxonomy"]["taxonomyCategories"]
        if (
            len(categories) == 6
            and categories[0]["category"] == "GCP"
            and categories[1]["category"] == "Compute"
            and categories[2]["category"] == "GCE"
            and categories[3]["category"] in {"VMs Preemptible", "VMs On Demand"}
            and categories[4]["category"] in {"Memory: Per GB", "Cores: Per Core", "Cores: 1 to 64"}
            and "Custom" not in sku["displayName"]
            and "Sole Tenancy" not in sku["displayName"]
            and categories[5]["category"] != "Cross VM"
        ):
            info_type = "VMs"
        elif (
            len(categories) == 5
            and categories[0]["category"] == "GCP"
            and categories[1]["category"] == "Compute"
            and categories[2]["category"] == "GPUs"
            and categories[3]["category"] in {"GPUs Preemptible", "GPUs On Demand"}
        ):
            info_type = "GPUs"
        elif (
            len(categories) == 6
            and categories[0]["category"] == "GCP"
            and categories[1]["category"] == "Compute"
            and categories[2]["category"] == "GPUs"
            and categories[3]["category"] in {"GPUs Preemptible", "GPUs On Demand"}
        ):
            info_type = "GPUs_with_Core_and_Memory"
        else:
            continue

        if info_type == "VMs":
            machine_family = categories[5]["category"]
            price_resource = categories[4]["category"]
            if price_resource == "Cores: 1 to 64":
                machine_family = "A2"
            price_resource = price_resource.split(":")[0]
            machine_model = "Standard" if "Custom" not in sku["displayName"] else "Custom"
            price_model = "On-demand" if categories[3]["category"] == "VMs On Demand" else "Preemptible"
            geo_type = sku["geoTaxonomy"]["type"]
            if geo_type == "TYPE_REGIONAL":
                region = sku["geoTaxonomy"]["regionalMetadata"]["region"]["region"]
                sku_infos.append(
                    {
                        "skuId": sku["skuId"],
                        "machineFamily": machine_family,
                        "machineModel": machine_model,
                        "region": region,
                        "priceModel": price_model,
                        "priceResource": price_resource,
                        "displayName": sku["displayName"],
                    }
                )
            elif geo_type == "TYPE_MULTI_REGIONAL":
                for region in sku["geoTaxonomy"]["multiRegionalMetadata"]["regions"]:
                    sku_infos.append(
                        {
                            "skuId": sku["skuId"],
                            "machineFamily": machine_family,
                            "machineModel": machine_model,
                            "region": region["region"],
                            "priceModel": price_model,
                            "priceResource": price_resource,
                            "displayName": sku["displayName"],
                        }
                    )
        elif info_type == "GPUs":
            gpu_type = categories[4]["category"]
            price_model = "On-demand" if categories[3]["category"] == "GPUs On Demand" else "Preemptible"
            geo_type = sku["geoTaxonomy"]["type"]
            if geo_type == "TYPE_REGIONAL":
                region = sku["geoTaxonomy"]["regionalMetadata"]["region"]["region"]
                gpu_sku_infos.append(
                    {
                        "skuId": sku["skuId"],
                        "gpuType": gpu_type,
                        "region": region,
                        "priceModel": price_model,
                        "priceResource": "GPU",
                        "displayName": sku["displayName"],
                    }
                )
            elif geo_type == "TYPE_MULTI_REGIONAL":
                for region in sku["geoTaxonomy"]["multiRegionalMetadata"]["regions"]:
                    gpu_sku_infos.append(
                        {
                            "skuId": sku["skuId"],
                            "gpuType": gpu_type,
                            "region": region["region"],
                            "priceModel": price_model,
                            "priceResource": "GPU",
                            "displayName": sku["displayName"],
                        }
                    )
        elif info_type == "GPUs_with_Core_and_Memory":
            gpu_type = categories[4]["category"]
            price_model = "On-demand" if categories[3]["category"] == "GPUs On Demand" else "Preemptible"
            price_resource = categories[5]["category"].split(":")[0]
            if price_resource == "GPU slice":
                continue
            geo_type = sku["geoTaxonomy"]["type"]
            if geo_type == "TYPE_REGIONAL":
                region = sku["geoTaxonomy"]["regionalMetadata"]["region"]["region"]
                gpu_sku_infos.append(
                    {
                        "skuId": sku["skuId"],
                        "gpuType": gpu_type,
                        "priceModel": price_model,
                        "priceResource": price_resource,
                        "region": region,
                        "displayName": sku["displayName"],
                    }
                )
            elif geo_type == "TYPE_MULTI_REGIONAL":
                for region in sku["geoTaxonomy"]["multiRegionalMetadata"]["regions"]:
                    gpu_sku_infos.append(
                        {
                            "skuId": sku["skuId"],
                            "gpuType": gpu_type,
                            "priceModel": price_model,
                            "priceResource": price_resource,
                            "region": region["region"],
                            "displayName": sku["displayName"],
                        }
                    )
    return sku_infos, gpu_sku_infos


def get_price_infos(
    response: dict,
    sku_ids: set[str],
    gpu_sku_ids: set[str],
) -> tuple[list[dict], list[dict]]:
    prices = response["prices"]
    price_infos = []
    gpu_price_infos = []
    for price in prices:
        sku_id = price["name"].split("/")[1]
        if sku_id not in sku_ids and sku_id not in gpu_sku_ids:
            continue

        rate_info = price.get("rate")
        if not isinstance(rate_info, dict):
            continue

        unit_info = rate_info.get("unitInfo", {})
        unit_quantity = unit_info.get("unitQuantity", {})
        price_value = _extract_price_value(price)
        record = {
            "skuId": sku_id,
            "currencyCode": price["currencyCode"],
            "price": price_value,
            "unit": unit_info.get("unit"),
            "unitQuantity": unit_quantity.get("value"),
        }
        if sku_id in sku_ids:
            price_infos.append(record)
            sku_ids.remove(sku_id)
        elif sku_id in gpu_sku_ids:
            gpu_price_infos.append(record)
            gpu_sku_ids.remove(sku_id)
    return price_infos, gpu_price_infos


def _extract_price_value(price: dict) -> float | None:
    try:
        list_price = price["rate"]["tiers"][0]["listPrice"]
        units = float(list_price.get("units", 0) or 0)
        nanos = float(list_price.get("nanos", 0) or 0) * 1e-9
        return units + nanos
    except Exception:
        return None


def _rows_to_frame(rows: list[dict], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    series = [
        pl.Series(column, [row.get(column) for row in rows], dtype=dtype, strict=False)
        for column, dtype in schema.items()
    ]
    return pl.DataFrame(series).select(list(schema.keys()))


def _first_price_expr(price_model: str, price_resource: str, alias: str) -> pl.Expr:
    return (
        pl.col("price")
        .filter((pl.col("priceModel") == price_model) & (pl.col("priceResource") == price_resource))
        .first()
        .alias(alias)
    )


def _calculate_price_expr(core_col: str, memory_col: str, gpu_col: str, alias: str) -> pl.Expr:
    core = pl.col(core_col)
    memory = pl.col(memory_col)
    gpu = pl.col(gpu_col).fill_null(0.0)
    return (
        pl.when(core.is_null() & memory.is_null())
        .then(pl.lit(None, dtype=pl.Float64))
        .when(core.is_null())
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(
            pl.max_horizontal(pl.col("vcpus"), pl.lit(1)) * core
            + pl.col("memory") * memory.fill_null(0.0)
            + pl.col("gpuCount").cast(pl.Float64) * gpu
        )
        .alias(alias)
    )


# GCP Billing API returns duplicate SKUs for the same group key with
# different prices.  Known cases (ref: github.com/Cyclenerd/
# google-cloud-pricing-cost-calculator/blob/master/build/README.md):
#   asia-northeast1: "running in Tokyo" vs "running in Japan"
#   us-east4:        "running in Northern Virginia" vs "running in Virginia"
#   asia-southeast1: two identical "running in Singapore" with different prices
# Rule: prefer city over country/state name; if names are identical, keep the
# more expensive SKU.
_PREFER_CITY = {
    "Japan": "Tokyo",
    "Virginia": "Northern Virginia",
}


def _dedup_skus(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    """Remove duplicate SKUs within each *group_cols* combination.

    Requires ``displayName`` and ``price`` columns (join price before calling).
    """
    dup_mask = df.group_by(group_cols).agg(pl.len().alias("_n")).filter(pl.col("_n") > 1).drop("_n")
    if dup_mask.is_empty():
        return df

    no_dup = df.join(dup_mask, on=group_cols, how="anti")
    has_dup = df.join(dup_mask, on=group_cols, how="semi")

    # Score each row: +1 if displayName contains a preferred city name,
    # so city-labeled SKUs sort first (descending).
    city_score = pl.lit(0)
    for country, city in _PREFER_CITY.items():
        city_score = pl.when(pl.col("displayName").str.contains(city)).then(1).otherwise(city_score)

    kept = (
        has_dup
        .with_columns(city_score.alias("_city"))
        .sort(
            group_cols + ["_city", "price"],
            descending=[False] * len(group_cols) + [True, True],
        )
        .unique(subset=group_cols, keep="first")
        .drop("_city")
    )

    return pl.concat([no_dup, kept], how="align")


def build_snapshot_df(
    sku_infos: list[dict],
    gpu_sku_infos: list[dict],
    price_infos: list[dict],
    gpu_price_infos: list[dict],
    machine_types_infos: list[dict],
    *,
    timestamp: datetime,
) -> pl.DataFrame:
    sku_df = _rows_to_frame(sku_infos, SKU_SCHEMA).sort(
        ["machineFamily", "region", "priceModel", "priceResource"]
    )
    gpu_sku_df = _rows_to_frame(gpu_sku_infos, GPU_SKU_SCHEMA).sort(
        ["gpuType", "region", "priceModel", "priceResource"]
    )
    price_df = _rows_to_frame(price_infos, PRICE_SCHEMA)
    gpu_price_df = _rows_to_frame(gpu_price_infos, PRICE_SCHEMA)
    machine_types_df = _rows_to_frame(machine_types_infos, MACHINE_TYPE_SCHEMA).sort(
        ["machineFamily", "machineType", "region", "vcpus", "memory"]
    )
    if machine_types_df.is_empty():
        return pl.DataFrame(schema=SNAPSHOT_SCHEMA)

    machine_types_df = machine_types_df.with_columns(pl.lit("Standard").alias("machineModel"))

    sku_df = _dedup_skus(
        sku_df.join(price_df, on="skuId", how="left"),
        ["machineFamily", "region", "priceModel", "priceResource"],
    ).drop(["currencyCode", "price", "unit", "unitQuantity"])
    gpu_sku_df = _dedup_skus(
        gpu_sku_df.join(gpu_price_df, on="skuId", how="left"),
        ["gpuType", "region", "priceModel", "priceResource"],
    ).drop(["currencyCode", "price", "unit", "unitQuantity"])

    total_df = sku_df.join(price_df, on="skuId", how="left")
    gpu_df = gpu_sku_df.join(gpu_price_df, on="skuId", how="left")

    vm_prices = total_df.group_by(["machineFamily", "machineModel", "region"]).agg(
        [
            _first_price_expr("On-demand", "Cores", "ondemandCorePrice"),
            _first_price_expr("On-demand", "Memory", "ondemandMemoryPrice"),
            _first_price_expr("Preemptible", "Cores", "preemptibleCorePrice"),
            _first_price_expr("Preemptible", "Memory", "preemptibleMemoryPrice"),
        ]
    )
    gpu_prices = gpu_df.group_by(["gpuType", "region"]).agg(
        [
            _first_price_expr("On-demand", "Cores", "ondemandCorePrice_new"),
            _first_price_expr("On-demand", "GPU", "ondemandGPUPrice"),
            _first_price_expr("On-demand", "Memory", "ondemandMemoryPrice_new"),
            _first_price_expr("Preemptible", "Cores", "preemptibleCorePrice_new"),
            _first_price_expr("Preemptible", "GPU", "preemptibleGPUPrice"),
            _first_price_expr("Preemptible", "Memory", "preemptibleMemoryPrice_new"),
        ]
    )

    df_final = machine_types_df.join(
        vm_prices,
        on=["machineFamily", "machineModel", "region"],
        how="left",
    ).join(
        gpu_prices,
        on=["gpuType", "region"],
        how="left",
    )

    for column in [
        "ondemandCorePrice",
        "ondemandMemoryPrice",
        "preemptibleCorePrice",
        "preemptibleMemoryPrice",
    ]:
        df_final = df_final.with_columns(
            pl.coalesce(pl.col(column), pl.col(f"{column}_new")).alias(column)
        )

    df_final = df_final.drop(
        [
            "ondemandCorePrice_new",
            "ondemandMemoryPrice_new",
            "preemptibleCorePrice_new",
            "preemptibleMemoryPrice_new",
        ]
    )

    ts_utc = timestamp.astimezone(timezone.utc) if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    df_final = (
        df_final.with_columns(
            [
                _calculate_price_expr(
                    "ondemandCorePrice",
                    "ondemandMemoryPrice",
                    "ondemandGPUPrice",
                    "ondemandPrice",
                ),
                _calculate_price_expr(
                    "preemptibleCorePrice",
                    "preemptibleMemoryPrice",
                    "preemptibleGPUPrice",
                    "preemptiblePrice",
                ),
            ]
        )
        .with_columns(
            [
                pl.lit(ts_utc.strftime("%Y-%m-%d %H:%M:%S")).alias("Time"),
                (
                    (pl.col("ondemandPrice") - pl.col("preemptiblePrice"))
                    / pl.col("ondemandPrice")
                    * 100
                ).alias("Savings"),
            ]
        )
        .select(
            [
                "Time",
                pl.col("machineType").alias("InstanceType"),
                pl.col("region").alias("Region"),
                pl.col("ondemandPrice").alias("OnDemand Price"),
                pl.col("preemptiblePrice").alias("Spot Price"),
                "Savings",
            ]
        )
        .drop_nulls()
    )

    return df_final

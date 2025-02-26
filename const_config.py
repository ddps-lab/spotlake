# Decorator
def constant(func):
    def func_set(self, value):
        raise TypeError

    def func_get(self):
        return func()

    return property(func_get, func_set)


class Storage(object):
    @constant
    def BUCKET_NAME():
        return "spotlake"

    @constant
    def DATABASE_NAME():
        return "spotlake"

    @constant
    def AWS_TABLE_NAME():
        return "aws"

    @constant
    def AZURE_TABLE_NAME():
        return "azure"

    @constant
    def GCP_TABLE_NAME():
        return "gcp"


class AwsCollector(object):
    @constant
    def LOCAL_PATH():
        return "/home/ubuntu/spotlake/collector/spot-dataset/aws/ec2_collector"

    @constant
    def S3_LATEST_DATA_SAVE_PATH():
        return "latest_data/latest_aws.json"

    @constant
    def S3_LOCAL_FILES_SAVE_PATH():
        return "rawdata/aws/localfile"

    @constant
    def S3_WORKLOAD_SAVE_PATH():
        return "rawdata/aws/workloads"


class AzureCollector(object):
    @constant
    def SLACK_WEBHOOK_URL():
        return ""

    @constant
    def GET_EVICTION_RATE_URL():
        return "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"

    @constant
    def GET_HARDWAREMAP_URL():
        return "https://afd.hosting.portal.azure.net/compute/?environmentjson=true&extensionName=Microsoft_Azure_Compute&l=en&trustedAuthority=portal.azure.com"

    @constant
    def GET_PRICE_URL():
        return "https://s2.billing.ext.azure.com/api/Billing/Subscription/GetSpecsCosts?SpotPricing=true"

    @constant
    def AZURE_SUBSCRIPTION_ID():
        return ""

    @constant
    def SPEC_RESOURCE_SETS_LIMIT():
        return 2000

    @constant
    def LATEST_PRICE_SAVING_IF_FILENAME():
        return "latest_price_saving_if.json"

    @constant
    def LATEST_PRICE_SAVING_IF_PKL_GZIP_FILENAME():
        return "latest_price_saving_if.pkl.gz"

    @constant
    def S3_LATEST_PRICE_SAVING_IF_DATA_SAVE_PATH():
        return "latest_data/latest_price_saving_if.json"

    @constant
    def S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH():
        return "latest_data/latest_price_saving_if.pkl.gz"

    @constant
    def QUERY_SELECTOR_FILENAME():
        return "query-selector-azure.json"

    @constant
    def S3_QUERY_SELECTOR_SAVE_PATH():
        return "query-selector/query-selector-azure.json"

    @constant
    def SERVER_SAVE_DIR():
        return "/tmp"

    @constant
    def SERVER_SAVE_FILENAME():
        return "latest_azure_df.pkl"

    @constant
    def GET_PRICE_URL():
        return "https://prices.azure.com:443/api/retail/prices?$filter=serviceName eq 'Virtual Machines' and priceType eq 'Consumption' and unitOfMeasure eq '1 Hour' and  contains(productName, 'Windows') eq false and contains(meterName, 'Low Priority') eq false  and contains(meterName, 'Expired') eq false and contains(location, 'Gov') eq false and contains(location, 'ATT') eq false &$skip="

    @constant
    def FILTER_LOCATIONS():
        return ['GOV', 'EUAP', 'ATT', 'SLV', '']

    @constant
    def MAX_SKIP():
        return 200

    @constant
    def SPOT_DATA_COLLECTION_LOG_GROUP_NAME():
        return "Collection-Data-Count"

    @constant
    def LOG_STREAM_NAME():
        return "Azure-Count"

    @constant
    def LOCATIONS_CALL_HISTORY_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/locations_call_history.json"

    @constant
    def LOCATIONS_OVER_LIMIT_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/locations_over_limit.json"

    @constant
    def INVALID_REGIONS_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/invalid_regions.json"

    @constant
    def INVALID_INSTANCE_TYPES_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/invalid_instance_types.json"

    @constant
    def LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/last_subscription_id_and_location.json"

    @constant
    def REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME():
        return "sps-collector/azure/saved_variable/region_map_and_instance_map.json"

    @constant
    def DF_TO_USE_TODAY_PKL_FILENAME():
        return "sps-collector/azure/df_to_use_today.pkl"

    @constant
    def S3_LATEST_DESIRED_COUNT_1_DATA_AVAILABILITYZONE_TRUE_SAVE_PATH():
        return "latest_data/latest_azure.json"

    @constant
    def S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH():
        return "latest_data/latest_sps_zone_true_azure.pkl.gz"

    @constant
    def S3_RAW_DATA_PATH():
        return "rawdata/azure"

class GcpCollector(object):
    @constant
    def API_LINK():
        return "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

    @constant
    def S3_LATEST_DATA_SAVE_PATH():
        return "latest_data/latest_gcp.json"

    @constant
    def LOCAL_PATH():
        return "/tmp"

    @constant
    def SPOT_DATA_COLLECTION_LOG_GROUP_NAME():
        return "Collection-Data-Count"

    @constant
    def LOG_STREAM_NAME():
        return "GCP-Count"
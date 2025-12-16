class AzureCollector:
    def __init__(self):
        self.MAX_SKIP = 2000
        self.GET_PRICE_URL = "https://prices.azure.com/api/retail/prices?currencyCode='USD'&$filter=serviceName eq 'Virtual Machines' and priceType eq 'Consumption'&$skip="
        self.FILTER_LOCATIONS = ['GOV', 'DoD', 'China', 'Germany']
        
        self.SERVER_SAVE_DIR = "/tmp"
        self.LATEST_PRICE_SAVING_IF_FILENAME = "latest_azure.json"
        self.LATEST_PRICE_SAVING_IF_PKL_GZIP_FILENAME = "latest_azure.pkl.gz"
        self.SERVER_SAVE_FILENAME = "latest_azure.pkl"

        # S3 Paths for SPS State
        self.S3_RAW_DATA_PATH = "rawdata/azure"
        self.S3_SAVED_VARIABLE_PATH = "rawdata/azure/sps/state"
        
        self.S3_REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME = "region_map_and_instance_map.json"
        
        self.S3_DF_TO_USE_TODAY_PKL_FILENAME = "rawdata/azure/sps/state/today_request_pool.pkl"
        
        self.S3_INVALID_REGIONS_JSON_FILENAME = "invalid_regions.json"
        self.S3_INVALID_INSTANCE_TYPES_JSON_FILENAME = "invalid_instance_types.json"
        self.S3_AVAILABLE_LOCATIONS_JSON_FILENAME = "available_locations.json"
        self.S3_LOCATIONS_CALL_HISTORY_JSON_FILENAME = "locations_call_history.json"
        self.S3_LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME = "last_subscription_id_and_location.json"
        self.S3_LOCATIONS_OVER_LIMIT_JSON_FILENAME = "locations_over_limit.json"
        
        self.ERROR_LOCATIONS_CALL_HISTORY_JSON_PATH = "rawdata/azure/sps/error/locations_call_history"
        
        self.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH = "latest_data/azure/price_saving_if.pkl.gz"
        self.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH = "latest_data/azure/all_data_az_true.pkl.gz"

        self.S3_LATEST_DESIRED_COUNT_1_DATA_AVAILABILITYZONE_TRUE_SAVE_PATH = "latest_data/azure/sorted_desired_count_1_az_true.json"
        self.S3_QUERY_SELECTOR_SAVE_PATH = "query-selector/azure/query_selector.json"

class Storage:
    def __init__(self):
        self.READ_BUCKET_NAME = "spotlake"
        self.WRITE_BUCKET_NAME = "spotlake-test"
        self.READ_DATABASE_NAME = 'spotlake'
        self.WRITE_DATABASE_NAME = 'spotlake-test'
        self.READ_TABLE_NAME = 'azure'
        self.WRITE_TABLE_NAME = 'azure-test'
        
        # Aliases for backward compatibility or default usage
        self.BUCKET_NAME = self.WRITE_BUCKET_NAME
        self.DATABASE_NAME = self.WRITE_DATABASE_NAME
        self.TABLE_NAME = self.WRITE_TABLE_NAME
        
        self.SPOT_DATA_COLLECTION_LOG_GROUP_NAME = "Collection-Data-Count"
        self.LOG_STREAM_NAME = "Azure-Count"

AZURE_CONST = AzureCollector()
STORAGE_CONST = Storage()

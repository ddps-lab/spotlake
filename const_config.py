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
    def LATEST_FILENAME ():
        return "latest_azure.json"

    @constant
    def S3_LATEST_DATA_SAVE_PATH():
        return "latest_data/latest_azure.json"
    
    @constant
    def QUERY_SELECTOR_FILENAME():
        return "query-selector-azure.json"
    
    @constant
    def S3_QUERY_SELECTOR_SAVE_PATH():
        return "query-selector/query-selector-azure.json"

    @constant
    def DF_WORKLOAD_COLS():
        return ['InstanceTier', 'InstanceType', 'Region']

    @constant
    def DF_FEATURE_COLS():
        return ['OndemandPrice', 'SpotPrice', 'IF']

    @constant
    def SERVER_SAVE_DIR():
        return "/tmp"

    @constant
    def SERVER_SAVE_FILENAME():
        return "latest_azure_df.pkl"

    @constant
    def GET_PRICE_URL():
        return "https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip="
    
    @constant
    def FILTER_LOCATIONS():
        return ['GOV', 'EUAP', 'ATT', 'SLV', '']
    
    @constant
    def MAX_SKIP():
        return 2000

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
        

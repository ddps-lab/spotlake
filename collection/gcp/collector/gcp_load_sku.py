from google.cloud import billing_v1
from google.protobuf.json_format import MessageToDict
import pandas as pd
from datetime import datetime
import pickle
import os


def get_service_list():
    # get service list by using google cloud billing catalog API
    # input : None
    # output : billing_v1.services.cloud_catalog.pagers.ListServicesPage

    service_request = billing_v1.ListServicesRequest()
    return client.list_services(request=service_request)


def get_skus_list(service_name):
    # get sku list by using google cloud billing catalog API
    # input : service_name(str)
    # output : list

    page_result_list = []
    sku_request = billing_v1.ListSkusRequest(parent=service_name)
    page_result = client.list_skus(request=sku_request)
    page_result_list.append(page_result.skus)

    while(page_result.next_page_token != ''):
        sku_request = billing_v1.ListSkusRequest(
            parent=service_name, page_token=page_result.next_page_token)
        page_result = client.list_skus(request=sku_request)
        page_result_list.append(page_result.skus)

    return page_result_list


def make_information_dict(key_list, content_list):
    # make information dictionary to put in dataframe
    # input: value_list(key list of new dict), content_list(value list of new dict)
    # output: dictionary

    info_dictionary = dict()

    for key in key_list:
        info_dictionary[key] = []

    for content in content_list:
        for key in key_list:
            info_dictionary[key].append(getattr(content, key))

    return info_dictionary


def make_dataframe(dataframe, info_dictionary):
    # make dataframe with information dictionaries
    # input : dataframe(service or sku), info_dictionary(each value type is list)
    # output : result dataframe

    for key in info_dictionary.keys():
        dataframe[key] = info_dictionary[key]

    return dataframe


if __name__ == '__main__':
    date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    global client
    client = billing_v1.CloudCatalogClient()

    # get service list and make dictionary to store service information
    service_list = get_service_list()
    service_keys = [
        'name', 'service_id',
        'display_name', 'business_entity_name'
    ]
    service_info_dict = make_information_dict(service_keys, service_list)

    # make service dataframe
    df_service = pd.DataFrame()
    df_service = make_dataframe(df_service, service_info_dict)

    # extract service to get sku list
    query_service_list = []
    query_service_list.append(
        df_service.loc[df_service['display_name'] == 'Compute Engine'])

    # get sku list
    sku_page_result = []
    for service in query_service_list:
        sku_page_result.append(get_skus_list(service['name'].values[0]))

    sku_list = []
    for service in sku_page_result:
        for page in service:
            for sku in page:
                sku_list.append(sku)

    # make dictionaries to store sku information
    sku_keys = [
        'category', 'name', 'sku_id', 'description',
        'service_regions', 'pricing_info', 'service_provider_name'
    ]
    sku_info_dict = make_information_dict(sku_keys, sku_list)

    # category
    category_keys = [
        'service_display_name', 'resource_family',
        'resource_group', 'usage_type'
    ]
    sku_category_dict = make_information_dict(
        category_keys, sku_info_dict['category'])
    del sku_info_dict['category']

    # pricing_info
    new_pricing_info_list = []
    for pricing_info in sku_info_dict['pricing_info']:
        tmp_price = list(pricing_info)[0]
        new_pricing_info_list.append(tmp_price)
    sku_info_dict['pricing_info'] = new_pricing_info_list

    pricing_info_keys = [
        'effective_time', 'pricing_expression',
        'aggregation_info', 'currency_conversion_rate'
    ]
    sku_pricing_info_dict = make_information_dict(
        pricing_info_keys, sku_info_dict['pricing_info'])
    del sku_info_dict['pricing_info']

    # pricing_expression
    pricing_exp_keys = [
        'usage_unit', 'usage_unit_description',
        'base_unit', 'base_unit_description',
        'base_unit_conversion_factor', 'display_quantity', 'tiered_rates'
    ]
    sku_pricing_exp_dict = make_information_dict(
        pricing_exp_keys, sku_pricing_info_dict['pricing_expression'])
    del sku_pricing_info_dict['pricing_expression']

    # aggregation_info
    aggregation_keys = [
        'aggregation_level',
        'aggregation_interval',
        'aggregation_count'
    ]
    sku_aggr_info_dict = make_information_dict(
        aggregation_keys, sku_pricing_info_dict['aggregation_info'])
    del sku_pricing_info_dict['aggregation_info']

    # service_regions : change Repeated type into list type
    new_region_list = []
    for service_regions in sku_info_dict['service_regions']:
        tmp_region_list = list(service_regions)
        new_region_list.append(tmp_region_list)
    sku_info_dict['service_regions'] = new_region_list

    # change tiered rates into list
    new_tiered_rates_list = []
    for rates in sku_pricing_exp_dict['tiered_rates']:
        new_tiered_rates_list.append(list(rates))
    sku_pricing_exp_dict['tiered_rates'] = new_tiered_rates_list

    # make sku dataframe
    df_sku = pd.DataFrame()
    dictionary_list = [
        sku_category_dict,
        sku_info_dict,
        sku_pricing_info_dict,
        sku_pricing_exp_dict,
        sku_aggr_info_dict
    ]
    for dictionary in dictionary_list:
        df_sku = make_dataframe(df_sku, dictionary)
    df_sku['timestamp'] = date_time

    # save as pickle
    save_path = '../../../../gcp_price_data'
    if os.path.isdir(save_path) == False:
        os.mkdir(save_path)

    with open(save_path + '/gcp_price.pkl', 'wb') as f:
        pickle.dump(df_sku, f)

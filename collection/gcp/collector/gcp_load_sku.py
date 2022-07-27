from google.cloud import billing_v1
from google.protobuf.json_format import MessageToDict
import proto.marshal.collections.repeated as pr
import pandas as pd
from datetime import datetime
import pickle


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


def make_dataframe(value_list, content_list, date_time):
    # make dataframe from list of service or sku
    # input : value_list(list), content_list(list), date_time(str)
    # output : Dataframe

    for value in value_list:
        globals()['{}_list'.format(value)] = []

    timestamp_list = []

    for content in content_list:
        for value in value_list:
            try:
                put_value = getattr(content, value)
                if type(put_value) in (pr.Repeated, pr.RepeatedComposite):
                    put_value = list(put_value)

                globals()['{}_list'.format(value)].append(put_value)

            except:
                if hasattr(content, 'category') == True:
                    globals()['{}_list'.format(value)].append(
                        getattr(getattr(content, 'category'), value))
        timestamp_list.append(date_time)

    new_df = pd.DataFrame()
    for value in value_list:
        new_df[value] = globals()['{}_list'.format(value)]
    new_df['timestamp'] = timestamp_list

    return new_df


if __name__ == '__main__':
    date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    global client
    client = billing_v1.CloudCatalogClient()

    # get service list and make dataframe
    service_list = get_service_list()
    service_value_list = [
        'name', 'service_id',
        'display_name', 'business_entity_name'
    ]
    df_service = make_dataframe(service_value_list, service_list, date_time)

    # extract service to query skus
    query_service_list = []
    query_service_list.append(
        df_service.loc[df_service['display_name'] == 'Compute Engine'])

    # get sku list and make dataframe
    sku_page_result = []
    for service in query_service_list:
        sku_page_result.append(get_skus_list(service['name'].values[0]))

    sku_list = []
    for service in sku_page_result:
        for page in service:
            for sku in page:
                sku_list.append(sku)

    sku_value_list = [
        'service_display_name',
        'name', 'sku_id', 'description',
        'resource_family', 'resource_group',
        'usage_type', 'service_regions',
        'pricing_info', 'service_provider_name'
    ]
    df_sku = make_dataframe(sku_value_list, sku_list, date_time)

    with open('C:/Users/wynter/Desktop/DDPS/gcp_price_data/gcp_price.pkl', 'wb') as f:
        pickle.dump(df_sku, f)

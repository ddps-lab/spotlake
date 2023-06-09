import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json

from utility import slack_msg_sender

def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 501, 502, 503, 504),
        session=None
):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_pricing_data(url):
    response = response = requests_retry_session().get(url)
    if response.status_code != 200:
        slack_msg_sender.send_slack_message(f"GCP get pricing data from VM instance pricing page : status code is {response.status_code}")
        raise Exception(f"GCP get pricing data : status code is {response.status_code}")
        
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    cloud_pricing_section = soup.find('div', 'cloud-section')
    pricing_table_list = cloud_pricing_section.find_all('cloudx-pricing-table')

    pricing_data = list()
    for table in pricing_table_list:
        table_content = table['layout']
        table_content = table_content.replace('\'', '"')
        table_content = table_content.replace('True', 'true')
        table_content = table_content.replace('False', 'false')
        dict_content = json.loads(table_content)

        if dict_content["rows"][0]["cells"][0] == "Machine type":
            pricing_data.append(dict_content)
    
    return pricing_data

def get_available_region_data(data):
    available_region_data = {}
    for table in data:
        rows = table['rows']
        for row in rows:
            if 'header' in row:
                continue
            
            machine_type = row['cells'][0]
            if 'Skylake Platform only' in machine_type:
                machine_type = machine_type.split(' ')[0]
            
            for value in row['cells']:
                if type(value) == dict:
                    available_region_data[machine_type] = list(value['priceByRegion'].keys())
                    break
    
    return available_region_data
import logging
import requests
from bs4 import BeautifulSoup

from gcp_metadata import region_mapping
from utility import slack_msg_sender


def get_url_list(page_url):
    # get iframe url list from VM Instance Pricing page
    # input : VM Instance Pricing page url
    # output : url list containing all iframe's url
    
    url_list = []
    response = requests.get(page_url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        iframe_list = soup.select('devsite-iframe')

        for iframe in iframe_list:
            url_list.append(iframe.select_one(
                'iframe').get_attribute_list('src')[0])
    else:
        slack_msg_sender.send_slack_message(response.status_code)
        logging.error(response.status_code)

    return url_list


def get_table(url):
    # get necessary table from html of iframe
    # input : url of iframe
    # output : table

    response = requests.get('https://cloud.google.com' + url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.select_one('table')

        if table.select_one('thead > tr > th').get_text() == 'Machine type':
            sample_type = table.select_one('tbody > tr > td').get_text()
            sample_type = sample_type.split('-')[0]
            if sample_type not in ('m2', 't2a'):
                return table
            else:
                return None
    else:
        slack_msg_sender.send_slack_message(response.status_code)
        logging.error(response.status_code)
        logging.error(url)


def extract_price(table, output):
    # get machine type and regional prices from table
    # input : table, dictionary to save price
    # output : dictionary that has new workload's regional prices

    # get index of machine type, ondemand, preemptible in table
    machine_type_idx = ''
    ondemand_idx = ''
    preemptible_idx = ''

    thead = table.find('thead')
    th_list = thead.select('th')
    for i in range(0, len(th_list)):
        if th_list[i].get_text().find('Machine type') != -1:
            machine_type_idx = i

        if th_list[i].get_text().find('Price (USD)') != -1:
            ondemand_idx = i
        elif th_list[i].get_text().find('On Demand') != -1:
            ondemand_idx = i
        elif th_list[i].get_text().find('On-demand') != -1:
            ondemand_idx = i

        if th_list[i].get_text().find('Spot') != -1:
            preemptible_idx = i

    # get price
    tbody_list = table.select('tbody')
    for tbody in tbody_list:
        tr_list = tbody.select('tr')
        for tr in tr_list:
            try:
                machine_type = tr.select('td')[machine_type_idx].get_text()
                ondemand_regional_price = tr.select('td')[ondemand_idx]
                preemptible_regional_price = tr.select('td')[preemptible_idx]

                if machine_type.find('Skylake Platform only') != -1:
                    machine_type = machine_type.split(
                        'Skylake Platform only')[0]
                elif machine_type.find('\n') != -1:
                    machine_type = machine_type.strip()

                for abbr, region in region_mapping.items():
                    attr = abbr + '-hourly'
                    ondemand_str = ondemand_regional_price[attr]
                    preemptible_str = preemptible_regional_price[attr]
                    ondemand_prc = ''
                    preemptible_prc = ''

                    if ondemand_str == 'Not available in this region':
                        ondemand_prc = -1
                    else:
                        ondemand_prc = float(ondemand_str.split('$')[1])

                    if preemptible_str == 'Not available in this region':
                        preemptible_prc = -1
                    else:
                        preemptible_prc = float(preemptible_str.split('$')[1])

                    output[machine_type][region]['ondemand'] = ondemand_prc
                    output[machine_type][region]['preemptible'] = preemptible_prc

            except IndexError as e:
                pass

            except Exception as e:
                slack_msg_sender.send_slack_message(e)
                logging.error(e)

    return output

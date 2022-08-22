import logging
import requests
from bs4 import BeautifulSoup

from gcp_metadata import region_mapping


def get_tbody(url):
    # get tbody from html of iframe
    # input : url of iframe
    # output : tbody

    response = requests.get('https://cloud.google.com' + url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        tbody = soup.select_one('tbody')
        return tbody
    else:
        logging.error(response.status_code)


def extract_price(tbody, output):
    # get machine type and regional prices from tbody
    # input : tbody, dictionary to save price
    # output : dictionary that has new workload's regional prices

    tr_list = tbody.select('tr')
    for tr in tr_list:
        try:
            machine_type = tr.select('td')[0].get_text()
            ondemand_regional_price = tr.select('td')[3]
            preemptible_regional_price = tr.select('td')[4]

            if machine_type.find('Skylake Platform only') != -1:
                machine_type = machine_type.split('Skylake Platform only')[0]
            elif machine_type.find('\n') != -1:
                machine_type = machine_type.strip()

            for abbr, region in region_mapping.items():
                attr = abbr + '-hourly'
                ondemand_str = ondemand_regional_price[attr]
                preemptible_str = preemptible_regional_price[attr]
                ondemand_prc = ''
                preemptible_prc = ''

                if ondemand_str == 'Not available in this region':
                    ondemand_prc = None
                else:
                    ondemand_prc = float(ondemand_str.split('$')[1])

                if preemptible_str == 'Not available in this region':
                    preemptible_prc = None
                else:
                    preemptible_prc = float(preemptible_str.split('$')[1])

                output[machine_type][region]['ondemand'] = ondemand_prc
                output[machine_type][region]['preemptible'] = preemptible_prc

        except IndexError as e:
            pass

        except Exception as e:
            logging.error(e)

    return output

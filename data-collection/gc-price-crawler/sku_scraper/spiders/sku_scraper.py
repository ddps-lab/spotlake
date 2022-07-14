import scrapy
from scrapy.exceptions import CloseSpider
from datetime import datetime
import os


class SkuScraperSpider(scrapy.Spider):
    search = 'preemptible'
    date_time = datetime.now()
    offset = 0
    limit = 100

    name = 'sku_scraper'
    allowed_domains = ['lepton.appspot.com']
    start_urls = ['http://lepton.appspot.com/']

    def start_requests(self):
        while(True):
            url = self.start_urls[0] + 'skus?format=skuPage&currency=USD&filter={}&offset={}&limit={}'.format(
                self.search, self.offset, self.limit)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # extract outer "skus"
        data_string = response.body.decode()
        skus_tmp = data_string.split('"skus":[', 1)[1]
        skus = skus_tmp.split('],"last_updated"')[0]

        self.log(len(skus))

        # write raw data
        # firstable, make directory
        make_path = '../../../../../gcp_raw_data'
        if os.path.isdir(make_path) == False:
            os.mkdir(make_path)

        # when skus is empty => finish
        if len(skus) <= 0:
            # trim to json format
            with open('{}/{}_{}_{}.json'.format(make_path, self.search, self.date_time.date(), self.date_time.time().strftime('%H-%M-%S')), 'r+') as f:
                temp_data = f.read()
                temp_data = temp_data.rstrip(',')
                final_json_data = '[' + temp_data + ']'
                f.truncate(0)
                f.seek(0)
                f.write(final_json_data)
            raise CloseSpider('Response is finished!')

        with open('{}/{}_{}_{}.json'.format(make_path, self.search, self.date_time.date(), self.date_time.time().strftime('%H-%M-%S')), 'a') as f:
            trim_data = data_string.lstrip(")]}'\n")
            trim_data += ','
            f.write(trim_data)

        self.log('Filename : {}, Write New Request for offset {}'.format(
            self.search, self.offset))

        self.offset += self.limit

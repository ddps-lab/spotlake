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

        # when skus is empty => finish
        if len(skus) <= 0:
            raise CloseSpider('Response is finished!')

        # write raw data
        # firstable, make directory
        make_path = '../../../../../gcp_raw_data'
        if os.path.isdir(make_path) == False:
            os.mkdir(make_path)
        with open('{}/{}_{}_{}.txt'.format(make_path, self.search, self.date_time.date(), self.date_time.time().strftime('%H-%M-%S')), 'a') as f:
            f.write(data_string)

        self.log('Filename : {}, Write New Request for offset {}'.format(
            self.search, self.offset))

        self.offset += self.limit

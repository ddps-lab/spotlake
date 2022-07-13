import scrapy
from scrapy.exceptions import CloseSpider


class SkuScraperSpider(scrapy.Spider):
    search = 'preemptible'
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
        with open('../../raw_data/{}.txt'.format(self.search), 'a') as f:
            f.write(data_string)

        self.log('Filename : {}, Write New Request for offset {}'.format(
            self.search, self.offset))

        self.offset += self.limit

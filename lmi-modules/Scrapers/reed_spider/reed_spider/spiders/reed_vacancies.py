# -*- coding: utf-8 -*-

from scrapy import Spider
from scrapy.http import Request
from scrapy import Request
from scrapy.selector import Selector
from selenium import webdriver


class ReedVacanciesSpider(Spider):
    name = 'reed-vacancies'
    allowed_domains = ['reed.co.uk']

    def start_requests(self):
        self.driver = webdriver.Chrome('C:\\Users\\User\\PycharmProjects\\labour-market-intelligence\\lmi-modules\\Scrapers\\chromedriver\\chromedriver')
        self.driver.get('https://www.reed.co.uk/jobs')

        sel = Selector(text=self.driver.page_source)
        vacancies = sel.xpath('//section/article')


        for vacancy in vacancies:
            extract_job_url =  vacancy.xpath('//div[2]/header/h3/a/@href').extract_first()
            job_url = 'https://www.reed.co.uk/' + extract_job_url

            yield Request(job_url, callback=self.parse_vacancy)


    def parse_vacancy(self, response):
        pass
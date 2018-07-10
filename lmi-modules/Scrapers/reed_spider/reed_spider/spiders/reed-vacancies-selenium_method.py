# -*- coding: utf-8 -*-

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from selenium import webdriver
from time import sleep
from selenium.common.exceptions import NoSuchElementException

class ReedVacanciesSpider(Spider):
    name = 'reed-vacancies'
    allowed_domains = ['reed.co.uk']

    def start_requests(self):
        self.driver = webdriver.Chrome('C:\\Users\\User\\PycharmProjects\\labour-market-intelligence\\lmi-modules\\Scrapers\\chromedriver\\chromedriver')
        self.driver.get('https://www.reed.co.uk/jobs')

        current_page = Selector(text=self.driver.page_source)
        job_urls = current_page.xpath('//h3/a/@href').extract()

        for job_url in job_urls:
            actual_url = 'https://www.reed.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy)

        sleep(15)
        while True:
            try:
                next_page = self.driver.find_element_by_xpath('//*[@class="next"]')
                sleep(25)
                self.logger.info('Sleeping for 5 seconds.')
                next_page.click()

                current_page = Selector(text=self.driver.page_source)
                job_urls = current_page.xpath('//h3/a/@href').extract()

                for job_url in job_urls:
                    actual_url = 'https://www.reed.co.uk' + job_url
                    yield Request(actual_url, callback=self.parse_vacancy)

            except NoSuchElementException:
                self.logger.info('No more pages to load.')
                self.driver.quit()
                break

    def parse_vacancy(self, response):
        country_stage1 = response.xpath('//*[@id="jobCountry"]')
        country_stage2 = country_stage1.xpath('//span/@value').extract_first()

        desc = response.xpath('//*[@itemprop="description"]').extract_first()
        date_posted = response.xpath('//*[@itemprop="datePosted"]/@content').extract_first()
        valid_till = response.xpath('//*[@itemprop="validThrough"]/@content').extract_first()


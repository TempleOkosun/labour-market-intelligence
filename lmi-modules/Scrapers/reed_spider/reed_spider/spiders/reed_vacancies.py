# -*- coding: utf-8 -*-

from scrapy import Spider
from scrapy.http import Request
import time
import random

class ReedVacanciesSpider(Spider):
    name = 'reed-vacancies'
    allowed_domains = ['reed.co.uk']
    start_urls =['http://reed.co.uk/jobs']

    def parse(self, response):
        job_urls = response.xpath('//h3/a/@href').extract()

        for job_url in job_urls:
            actual_url = 'https://www.reed.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy)



        # For Processing next page.
        next_page_url = response.xpath('//*[@id="nextPage"]/@href').extract_first()
        absolute_next_page_url = 'https://www.reed.co.uk' + next_page_url
        sleep_time = random.randint(10, 15)
        time.sleep(sleep_time)
        yield Request(absolute_next_page_url)


    def parse_vacancy(self, response):
        full_job_url = response.xpath('//*[@itemprop="url"]/@content').extract()[1]
        job_id = response.xpath('//*[@class="reference text-center"]/text()').extract_first()[11:]
        employment_type = response.xpath('//*[@itemprop="employmentType"]/text()').extract_first()
        location = response.xpath('//*[@itemprop="addressLocality"]/text()').extract_first()
        region = response.xpath('//*[@itemprop="addressRegion"]/text()').extract_first()
        salary = response.xpath('//*[@itemprop="baseSalary"]/span/text()').extract_first()
        posted_by = response.xpath('//*[@itemprop="name"]/text()').extract_first()
        job_poster_url = response.xpath('//*[@itemprop="url"]/@content').extract()[0]
        job_title = response.xpath('//*[@class="col-xs-12"]/h1/text()').extract_first()
        # job_title = response.xpath('//*[@itemprop="title"]/@content').extract_first()
        country = response.xpath('//*[@id="jobCountry"]/@value').extract_first()
        job_desc = response.xpath('//*[@itemprop="description"]').extract_first()
        date_posted = response.xpath('//*[@itemprop="datePosted"]/@content').extract_first()
        valid_till = response.xpath('//*[@itemprop="validThrough"]/@content').extract_first()
        industry = response.xpath('//*[@itemprop="industry"]/@content').extract_first()
        time_crawled = time.asctime()
        data_source = "reed.co.uk"

        yield {
            'Job_Url': full_job_url,
            'Job_Id': job_id,
            'Employment_Type': employment_type,
            'Location': location,
            'Region': region,
            'Salary': salary,
            'Posted_By': posted_by,
            'Job_Poster_Url': job_poster_url,
            'Job_Title': job_title,
            'Country': country,
            'Job_Description': job_desc,
            'Date_Posted': date_posted,
            'Valid_Till': valid_till,
            'Industry': industry,
            'Time_Crawled': time_crawled,
            'Data_Source': data_source
        }


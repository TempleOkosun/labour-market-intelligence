# -*- coding: utf-8 -*-

from scrapy import Spider
from scrapy.http import Request
import time
import random
from bs4 import BeautifulSoup


class ReedVacanciesSpider(Spider):

    name = 'reed-vacancies'
    allowed_domains = ['reed.co.uk']
    start_urls = ['http://reed.co.uk/jobs','https://www.reed.co.uk/jobs?datecreatedoffset=LastTwoWeeks',
                  'https://www.reed.co.uk/jobs?datecreatedoffset=LastWeek']

    def parse(self, response):
        # Get all the urls for the Job Ads on the page
        job_urls = response.xpath('//h3/a/@href').extract()

        # For each url, retrieve the full page-
        # (where we will get the complete details) and give it to the parser function.
        for job_url in job_urls:
            actual_url = 'https://www.reed.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy)


        # For Processing next page.
        # Get the url for next page.
        # I avoided while loop to check if the next page is available.
        # I certainly dont want to run the code till end of the database, that will be too much on the server.
        # NB: reed.co.uk shuffles the data view every time a new connection is started.

        next_page_url = response.xpath('//*[@id="nextPage"]/@href').extract_first()

        # Add the domain to make it a complete url
        absolute_next_page_url = 'https://www.reed.co.uk' + next_page_url

        #  Randomly delay the request a bit to avoid overloading.
        sleep_time = random.randint(5, 10)
        time.sleep(sleep_time)

        # Get the page and send to the parsing function
        yield Request(absolute_next_page_url)


    def parse_vacancy(self, response):

        # Returns a list of urls, the actual job url is in index [1]
        full_job_url = response.xpath('//*[@itemprop="url"]/@content').extract()[1]

        # Returns job id with some starting words that should be eliminated- "Reference: 35610799"
        job_id = response.xpath('//*[@class="reference text-center"]/text()').extract_first()[11:]

        # Returns employment type as a list [Permanent, Part-Time]
        employment_type = response.xpath('//*[@itemprop="employmentType"]/text()').extract()[0]
        employment_type_split = employment_type.split(",")
        job_type = employment_type_split[0]
        job_time = employment_type_split[1]

        # Returns the location i.e. nearest city eg.- Aberdeen
        location = response.xpath('//*[@itemprop="addressLocality"]/text()').extract_first()

        # Returns a region if available eg. North England.
        region = response.xpath('//*[@itemprop="addressRegion"]/@content').extract_first()

        # Returns the country eg.- Scotland
        country = response.xpath('//*[@id="jobCountry"]/@value').extract_first()

        # Returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = response.xpath('//*[@itemprop="baseSalary"]/span/text()').extract_first()

        # Returns the organization who posted it eg. 'NHS Highland'
        posted_by = response.xpath('//*[@itemprop="name"]/text()').extract_first()

        # Returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
        # It is the first item in the returned list of urls.
        job_poster_url = response.xpath('//*[@itemprop="url"]/@content').extract()[0]

        # Returns the full job-title eg. 'Senior Health & Social Care Support Worker'
        job_title = response.xpath('//*[@class="col-xs-12"]/h1/text()').extract_first()
        # Another alternative:  job_title = response.xpath('//*[@itemprop="title"]/@content').extract_first()

        # Returns the full job description with html tags.
        job_desc_with_html = response.xpath('//*[@itemprop="description"]').extract_first()
        # The lxml parser is used to remove html tags from the extracted job description.
        job_desc = BeautifulSoup(job_desc_with_html, "lxml").text


        # Returns the date job was posted.
        date_posted = response.xpath('//*[@itemprop="datePosted"]/@content').extract_first()

        # Returns the date job is valid till.
        valid_till = response.xpath('//*[@itemprop="validThrough"]/@content').extract_first()

        # Returns the job industry
        industry = response.xpath('//*[@itemprop="industry"]/@content').extract_first()

        # Adds the time crawled
        time_crawled = time.asctime()

        # Adds the name of the data source.
        data_source = "reed.co.uk"

        yield {
            'Job_Url': full_job_url,
            'Job_Id': job_id,
            'Job_Type': job_type,
            'Job_Time': job_time,
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


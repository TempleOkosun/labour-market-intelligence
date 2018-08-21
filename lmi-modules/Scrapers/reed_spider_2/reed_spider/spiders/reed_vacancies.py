# -*- coding: utf-8 -*-
"""
Three different Scrapers were developed to scrap reed.co.uk (requests were dropping at some point.) This is the second
module designed to scrap reed.co.uk. Design 2: Uses a for loop to manually iterate over next pages.

Returns:
    A dictionary of the data points specified to be collected into a MongoDB collection.
"""

import time
import random
from scrapy import Spider
from scrapy.http import Request
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from bs4 import BeautifulSoup


class ReedVacanciesSpider(Spider):
    """
    ReedVacanciesSpider defines how reed.co.uk will be scraped.
    It includes how to perform the crawl i.e following links, and how to extract structured data from it
    """
    name = 'reed-vacancies'
    allowed_domains = ['reed.co.uk']
    start_urls = ['http://reed.co.uk/jobs','https://www.reed.co.uk/jobs?datecreatedoffset=LastTwoWeeks',
                  'https://www.reed.co.uk/jobs?datecreatedoffset=LastWeek']

    # Request for the start urls. Send the response as an argument to default method 'parse' & to 'parse_vacancy' also.
    def parse(self, response):
        """
        This method Requests to crawl the start URLs, and specify a callback method 'parse_vacancy'
        to be called to extract data from the response object downloaded.
        A link to the next page is also retrieved in the response and used to schedule another request using the
        same 'parse' method as callback. This forms a loop that runs till there is no more pages.
        Finally, it sends details of any encountered errors to the parse_errors method.

        :return: response(web page) sent to the method 'parse' and 'parse_vacancy'
        """
        # xpath to get all the urls to the full job advert details page for the summarized job ads shown on start urls.
        job_urls = response.xpath('//h3/a/@href').extract()

        # For each url in the list, retrieve the page, i.e. the page to the job ads full description.
        # Return to the parse method (parse_vacancy)to extract data points.
        sleep_time = random.randint(0, 3)
        for job_url in job_urls:
            actual_url = 'https://www.reed.co.uk' + job_url
            time.sleep(sleep_time)
            yield Request(actual_url, callback=self.parse_vacancy, errback=self.parse_errors)

        # For Processing next page.
        # Get the url for next page.
        # NB: reed.co.uk shuffles the data view every time a new connection is started.
        next_page_url = response.xpath('//*[@id="nextPage"]/@href').extract()
        # Get the page and send to the parse method, A loop.
        for the_url in next_page_url:
            time.sleep(sleep_time)
            absolute_next_page_url = 'https://www.reed.co.uk' + the_url
            yield Request(absolute_next_page_url, callback=self.parse, dont_filter=False, errback=self.parse_errors)

    # scrapy filters out duplicated urls to sites already visited
    def parse_vacancy(self, response):
        """
        This method extracts the specified data points from the response object (web page)
        :param: response(web page)
        :return: A python dictionary of all extracted data points and sends the data to the database.
        """
        self.logger.info('Got successful response from {}'. format(response.url))
        # This xpath returns a list of urls, the actual job url is in index [1]
        full_job_url = response.xpath('//*[@itemprop="url"]/@content').extract()[1]

        # This xpath returns job id with some starting words that should be eliminated- "Reference: 35610799"
        job_id = response.xpath('//*[@class="reference text-center"]/text()').extract_first()[11:]

        # This xpath returns employment type as a list [Permanent, Part-Time]
        employment_type = response.xpath('//*[@itemprop="employmentType"]/text()').extract()[0]
        employment_type_split = employment_type.split(",")
        job_type = employment_type_split[0]
        job_time = employment_type_split[1]

        # This xpath returns the location i.e. nearest city eg.- Aberdeen
        location = response.xpath('//*[@itemprop="addressLocality"]/text()').extract_first()

        # This xpath returns a region if available eg. North England.
        region = response.xpath('//*[@itemprop="addressRegion"]/@content').extract_first()

        # This xpath returns the country eg.- Scotland
        country = response.xpath('//*[@id="jobCountry"]/@value').extract_first()

        # This xpath returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = response.xpath('//*[@itemprop="baseSalary"]/span/text()').extract_first()

        # This xpath returns the organization who posted it eg. 'NHS Highland'
        posted_by = response.xpath('//*[@itemprop="name"]/text()').extract_first()

        # This xpath returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
        # It is the first item in the returned list of urls.
        job_poster_url = response.xpath('//*[@itemprop="url"]/@content').extract()[0]

        # This xpath returns the full job-title eg. 'Senior Health & Social Care Support Worker'
        job_title = response.xpath('//*[@class="col-xs-12"]/h1/text()').extract_first()
        # Another alternative:  job_title = response.xpath('//*[@itemprop="title"]/@content').extract_first()

        # This xpath returns the full job description with html tags.
        job_desc_with_html = response.xpath('//*[@itemprop="description"]').extract_first()
        # The lxml parser is used to remove html tags from the extracted job description.
        job_desc = BeautifulSoup(job_desc_with_html, "lxml").text

        # This xpath returns the date job was posted.
        date_posted = response.xpath('//*[@itemprop="datePosted"]/@content').extract_first()

        # This xpath returns the date job is valid till.
        valid_till = response.xpath('//*[@itemprop="validThrough"]/@content').extract_first()

        # This xpath returns the job industry
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

    def parse_errors(self, failure):
        """
        This method examines the errors from the Requests in the parse method.
        """
        # Log all failures to scrapy console.
        self.logger.error(repr(failure))
        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)


if __name__ == 'main':
    ReedVacanciesSpider()

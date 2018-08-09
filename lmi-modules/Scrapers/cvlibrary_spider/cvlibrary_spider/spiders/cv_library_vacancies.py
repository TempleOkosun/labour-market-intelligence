# -*- coding: utf-8 -*-
"""
Main module for collecting data automatically from cv-library.co.uk

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


class CvLibraryVacanciesSpider(Spider):
    """
    CvLibraryVacanciesSpider defines how cv-library.co.uk will be scraped.
    It includes how to perform the crawl i.e following links, and how to extract structured data from it
    """
    name = 'cv-library-vacancies'
    allowed_domains = ['cv-library.co.uk']
    start_urls = ['https://www.cv-library.co.uk/search-jobs?posted=&search=1&fp=1&q=&geo=&distance=750&salarymin=&salarymax=&salarytype=annum&tempperm=Any']

    # Request for the start url. Send the response as an argument to default method 'parse' & to 'parse_vacancy' also.
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
        job_urls = response.xpath('//*[@id="js-jobtitle-details"]/a/@href').extract()

        # For each url in the list, retrieve the page, i.e. the page to the job ads full description.
        # Return to the parse method (parse_vacancy)to extract data points.
        for job_url in job_urls:
            actual_url = 'https://www.cv-library.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy, errback=self.parse_errors)

        # For Processing next page.
        # Get the url for next page.
        # NB: cv-library shuffles the data view every time a new connection is started.
        # The next page link is the last element returned in the list extracted by the xpath selector.

        # We will retrieve the list first.
        next_page_list = response.xpath('//*[@class="mmpage"]/a/@href').extract()
        # Then get the no of items in the list. i.e. len of list.
        next_page_list_length = len(next_page_list)
        # We will calculate the last element and use it to retrieve the link from the list.
        next_page_list_index = next_page_list_length - 1
        # We will pass the index no to the selector to extract it
        next_page_url = next_page_list[next_page_list_index]
        # We could also achieve all these by simply specifying first list element from the back with slice [-1]

        # Get the page and send to the parse method, A loop.
        if next_page_url is not None:
            # Add the domain to make it a complete url
            absolute_next_page_url = 'https://www.cv-library.co.uk' + next_page_url
            #  Randomly delay the request a bit to avoid overloading.
            sleep_time = random.randint(5, 10)
            time.sleep(sleep_time)
            # Get the page and send to the parsing function
            yield Request(absolute_next_page_url)

    def parse_vacancy(self, response):
        """
        This method extracts the specified data points from the response object (web page)
        :param response:
        :return: A python dictionary of all data points.
        """
        self.logger.info('Got successful response from {}'.format(response.url))

        # We need employment type Permanent, Part-time
        # This will return the job type i.e. Permanent
        # cv-library also has contract length but will be ignored as it is not available on others.

        # A json string with all jobs info is returned in the header of the page.
        # However, I'll only extract what cannot be accessed via direct class-name with string operations on the json
        json_string = response.xpath('//html/head/script').extract()
        json_string = json_string[0]

        # "JOB_TYPE": "Permanent", "PAGE_TYPE"
        # Look for where the job_type appears in the string. The index for the first character J is returned.
        job_type_start_forjobtype = (json_string.find('JOB_TYPE') + 11)
        # Look for where page_type appears in the string. The index for the first character P is returned.
        page_type_start_for_job_type = (json_string.find('PAGE_TYPE') - 3)
        # Get the word in between.
        job_type = json_string[job_type_start_forjobtype:page_type_start_for_job_type]

        # "JOB_TITLE": "Executive Director", "JOB_TYPE"
        # Look for where the job_title appears in the string. The index for the first character J is returned.
        job_title_end_for_job_title = (json_string.find('JOB_TITLE') + 12)
        # Look for where the job_type appears in the string. The index for the first character J is returned.
        job_type_start_for_job_title = (json_string.find('JOB_TYPE') -3)
        job_title = json_string[job_title_end_for_job_title:job_type_start_for_job_title]

        # "JOB_INDUSTRY":"Medical/Pharmaceutical/Scientific","USER_TYPE"
        job_industry_start_for_job_industry = (json_string.find('JOB_INDUSTRY') + 15)
        user_type_start_for_job_industry = (json_string.find('USER_TYPE') -3)
        job_industry = json_string[job_industry_start_for_job_industry:user_type_start_for_job_industry]

        # "START_DATE":"ASAP","JOB_INDUSTRY"
        start_date_begins = (json_string.find('START_DATE') + 13)
        job_industry_start_point = (json_string.find('JOB_INDUSTRY') -3)
        validity = json_string[start_date_begins: job_industry_start_point]

        # "JOB_TOWN":"Colchester","JOB_REFERENCE"
        start_job_town = (json_string.find('JOB_TOWN') + 11)
        start_job_reference = (json_string.find('JOB_REFERENCE') -3)
        location = json_string[start_job_town: start_job_reference]

        # "JOB_COUNTY":" Essex","JOB_COMPANY_ID"
        start_job_county = (json_string.find('JOB_COUNTY') + 13)
        start_job_company_id = (json_string.find('JOB_COMPANY_ID') - 3)
        region = json_string[start_job_county: start_job_company_id]

        # N/A not provided in their data model. A work around will be provided during data preparation.
        country = 'N/A'

        # Returns the full job description in a <div> with html tags as a list.
        job_desc_with_html = response.xpath('//*[@class="jd-details jobview-desc"]').extract()
        # Convert the list to string so that itcan be parsed.
        # The slicing will help remove [\'<div class="jd-details jobview-desc">\\n\\n\\n at begining and <div> at end
        job_desc_string = str (job_desc_with_html)[80:-20]
        # The lxml parser is used to remove html tags from the extracted job description.
        job_desc = BeautifulSoup(job_desc_string, "lxml").text

        # Returns the full job-title eg. 'Senior Health & Social Care Support Worker'
        # job_title = response.xpath('//h1[@class="jobTitle"]/text()').extract_first()

        # Returns the actual job url
        full_job_url = response.xpath('//*[@rel="canonical"]/@href').extract_first()
        # Returns job id
        job_id = response.xpath('//body/@data-id').extract_first()
        # Returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = response.xpath('//*[@id="job-salary"]/text()').extract()
        # Returns the organization who posted it eg. 'NHS Highland'
        posted_by = response.xpath('//*[@id="js-company-details"]/a/text()').extract_first()
        # Returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
        job_poster_url = response.xpath('//*[@id="js-company-details"]/a/@href').extract_first()

        # Returns the date job was posted.
        # comes out as: '\n   12/07/2018 (20:41)\n   n. The string splice will produce: '12/07/2018'
        date_posted  = (response.xpath('//*[@id="js-posted-details"]/text()').extract_first())[17:27]
        # Adds the time crawled
        time_crawled = time.asctime()
        # Adds the name of the data source.
        data_source = "cv-library.co.uk"

        yield {
            'Job_Url': full_job_url,
            'Job_Id': job_id,
            'Location': location,
            'Region': region,
            'Job_Type': job_type,
            'Salary': salary,
            'Posted_By': posted_by,
            'Job_Poster_Url': job_poster_url,
            'Job_Title': job_title,
            'Country': country,
            'Job_Description': job_desc,
            'Date_Posted': date_posted,
            'Valid_Till': validity,
            'Industry': job_industry,
            'Time_Crawled': time_crawled,
            'Data_Source': data_source
        }

    def parse_errors(self, failure):
        """
        This method examines the errors from the Requests in the parse method.
        """
        # Log all failures.
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

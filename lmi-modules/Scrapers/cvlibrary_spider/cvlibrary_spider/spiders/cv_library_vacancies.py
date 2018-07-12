# -*- coding: utf-8 -*-
import scrapy
from scrapy import Spider
from scrapy.http import Request
import time
import random

class CvLibraryVacanciesSpider(scrapy.Spider):
    name = 'cv-library-vacancies'
    allowed_domains = ['cv-library.co.uk']
    start_urls = ['https://www.cv-library.co.uk/search-jobs?posted=&search=1&fp=1&q=&geo=&distance=750&salarymin=&salarymax=&salarytype=annum&tempperm=Any']

    def parse(self, response):
        # Get all the urls for the Job Ads on the page
        job_urls = response.xpath('//*[@id="js-jobtitle-details"]/a/@href').extract()

        # For each url, retrieve the full page and give it to the parser function.
        for job_url in job_urls:
            actual_url = 'https://www.cv-library.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy)

        # For Processing next page.
        # Get the url for next page.
        # I avoided while loop to check if the next page is available.
        # I certainly do not want to run the code till end of the database, that will be too much on the server.
        # The next page link is the last element returned in the list extracted by the xpath selector.

        # We will retrieve the list first.
        next_page_list = response.xpath('//*[@class="mmpage"]/a/@href').extract()

        # Then get the no of items in the list. i.e. len of list.
        next_page_list_length = len(next_page_list)

        # We will calculate the last element and use it to retrieve the link from the list.
        next_page_list_index = next_page_list_length - 1

        # We will pass the index no to the selector to extract it
        next_page_url = next_page_list[next_page_list_index]

        # Add the domain to make it a complete url
        absolute_next_page_url = 'https://www.cv-library.co.uk' + next_page_url

        #  Randomly delay the request a bit to avoid overloading.
        sleep_time = random.randint(15, 20)
        time.sleep(sleep_time)

        # Get the page and send to the parsing function
        yield Request(absolute_next_page_url)

    def parse_vacancy(self, response):

        # Returns the actual job url
        full_job_url = response.xpath('//*[@rel="canonical"]/@href').extract_first()

        # Returns job id
        job_id = response.xpath('//body/@data-id').extract_first()

        # We need employment type as a list [Permanent, Part-Time]
        #  This will return the job type i.e. Permanet or Temporary.
        jobtype = response.xpath('//html/body/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[2]/div[5]/text()').extract_first()
        # Job time i.e. Full-time or Part time. The splicing helps return a cleaner result 'N/A \xa0'.
        # Especially when we have FullTime or PartTime as the time.
        contract_time = (response.xpath('//html/body/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[2]/div[9]/text()').extract_first())[:10]
        # We will now combine them to have a form similar to the other data sources.
        employment_type = jobtype + ', ' + contract_time

        # Returns the location i.e. nearest city eg.- Aberdeen
        location = response.xpath('//div[@id="job-location"]/text()').extract_first()

        # N/A.
        region = ''

        # N/A
        country = ''

        # Returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = response.xpath('//*[@id="job-salary"]/text()').extract()

        # Returns the organization who posted it eg. 'NHS Highland'
        posted_by = response.xpath('//*[@id="js-company-details"]/a/text()').extract_first()

        # Returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
        job_poster_url = response.xpath('//*[@id="js-company-details"]/a/@href').extract_first()

        # Returns the full job-title eg. 'Senior Health & Social Care Support Worker'
        job_title = response.xpath('//h1[@class="jobTitle"]/text()').extract_first()

        # Returns the full job description with html tags.
        job_desc = response.xpath('//*[@class="jd-details jobview-desc"]').extract()

        # Returns the date job was posted.
        # comes out as: '\n   12/07/2018 (20:41)\n   n. The string splice will produce: '12/07/2018'
        date_posted  = (response.xpath('//*[@id="js-posted-details"]/text()').extract_first())[17:27]

        # Returns the date job is valid till. N/A
        # start_date = (response.xpath('//html/body/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[2]/div[7]/text()').extract_first())[:5]

        # Returns the job industry. N/A

        # Adds the time crawled
        time_crawled = time.asctime()

        # Adds the name of the data source.
        data_source = "cv-library.co.uk"

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
            'Valid_Till': '',
            'Industry': '',
            'Time_Crawled': time_crawled,
            'Data_Source': data_source
        }


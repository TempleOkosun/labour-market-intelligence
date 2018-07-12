# -*- coding: utf-8 -*-
import scrapy
from datetime import date, timedelta
from scrapy import Spider
from scrapy.http import Request
import time
import random


class GlassdoorVacanciesSpider(scrapy.Spider):
    name = 'glassdoor-vacancies'
    allowed_domains = ['glassdoor.co.uk']

    # Initial page for all UK jobs over 150,000
    start_urls = ['https://www.glassdoor.co.uk/Job/uk-jobs-SRCH_IL.0,2_IN2.htm']

    def parse(self, response):
        # Get all the urls for the Job Ads on the page
        job_urls = response.xpath('//*[@class="flexbox"]/div/a/@href').extract()

        # For each url, retrieve the full page and give it to the parser function.
        for job_url in job_urls:
            actual_url = 'https://www.glassdoor.co.uk' + job_url
            yield Request(actual_url, callback=self.parse_vacancy)

        # For Processing next page. current_page + 1 till we reach the total no of pages. The pages follows that pattern
        # The total number of pages. Returns a list eg. [' Page 1 of 4897']. Extract and change to an int.
        total_pages = int(response.xpath('//*[@id="ResultsFooter"]/div/text()').extract_first()[11:])
        current_page_url = response.xpath('//*[@rel="canonical"]/@href').extract_first()

        # we split the different part of current url to get the page no.
        try:
            next_page_url_part1 = current_page_url[:58]
            next_page_url_part3 = current_page_url[59:]  # current_page_url[-4:]
            page_no = int(current_page_url[58:-4])

        except ValueError:
            page_no = 1


        next_page_no = page_no + 1
        next_page_no_str = str(next_page_no)
        next_page_url = next_page_url_part1 + next_page_no_str + next_page_url_part3


        # We use it as an absolute url directly no need for adding domain.
        absolute_next_page_url = next_page_url

        #  Randomly delay the request a bit to avoid overloading.
        sleep_time = random.randint(15, 20)
        time.sleep(sleep_time)

        # Get the page and send to the parsing function
        yield Request(absolute_next_page_url)


    def parse_vacancy(self, response):

        # Returns the actual job url is in index
        full_job_url = response.xpath('//*[@rel="canonical"]/@href').extract_first()

        # Returns JobDesc2838197211 but the [7:] will return-2838197211
        job_id = response.xpath('//div[@class="jobDesc "]/@id').extract_first()[7:]

        # Returns employment type as a list [Permanent, Part-Time]
        employment_type = ''

        # Returns the location i.e. nearest city eg.- Aberdeen
        location_1 = (response.xpath('//span[@class="subtle ib"]/text()').extract_first()).split()[1]
        # Location and Country are together "Edinburgh, Scotland". Need to split and remove comma
        location = location_1.split(',')[0]

        # Returns a region if available eg. North England.
        region = ''

        # Returns the country eg.- Scotland
        # Location and Country are together "Edinburgh, Scotland".
        # Need to split, remove comma and extract country which is 2 element in the list
        country = (response.xpath('//span[@class="subtle ib"]/text()').extract_first()).split()[1]

        # Returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = ''

        # Returns the organization who posted it eg. 'NHS Highland'
        posted_by = response.xpath('//span[@class="strong ib"]/text()').extract_first()

        # Returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
        job_poster_url = "https://glassdoor.co.uk" + response.xpath('//*[@class="sqLogoLink noMargTop"]/@href').extract_first()

        # Returns the full job-title eg. 'Senior Health & Social Care Support Worker'
        job_title = response.xpath('//h2[@class="noMargTop margBotXs strong"]/text()').extract_first()

        # Returns the full job description with html tags.
        job_desc = response.xpath('//div[@class="jobDesc "]/div').extract()

        # Returns the date job was posted.
        # Returns a  string eg. ' 7 days ago'. This will extract the digit part-[:3] and convert it to integer
        # We can then subtract the days to find the exact date.
        date_posted_unadjusted = int(response.xpath('//span[@class="minor nowrap"]/text()').extract_first()[:3])
        dt = date.today() - timedelta(date_posted_unadjusted)
        date_posted = str (dt)

        # Returns the date job is valid till.
        valid_till = ''

        # Returns the job industry
        industry = ''

        # Adds the time crawled
        time_crawled = time.asctime()

        # Adds the name of the data source.
        data_source = "glassdoor.co.uk"

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


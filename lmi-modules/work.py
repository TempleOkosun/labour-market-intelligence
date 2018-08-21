
import scrapy

class CvLibraryVacanciesSpider(scrapy.Spider):
    name = 'cv-library-vacancies'
    allowed_domains = ['cv-library.co.uk']
    start_urls = ['https://www.cv-library.co.uk/search-jobs?']
    # Request for the start url. Send the response as an argument to default method 'parse'
    def parse(self, response):
        # xpath to get all the urls to the full job advert details page for the summarized job ads shown on start urls.
        job_urls = response.xpath('//*[@id="js-jobtitle-details"]/a/@href').extract()

        # For each url in the list, retrieve the page, i.e. the page to the job ads full description.
        # Return to the method (parse_vacancy)to extract data points.
        for job_url in job_urls:
            actual_url = 'https://www.cv-library.co.uk' + job_url
            yield scrapy.Request(actual_url, callback=self.parse_vacancy, errback=self.parse_errors)

        # For Processing next page.
        next_page_url = response.xpath('//*[@class="mmpage"]/a/@href').extract_first()
        if next_page_url is not None:
            # Get the page and send to the parsing method parse
            yield scrapy.Request(next_page_url, callback=self.parse, dont_filter=False,  errback=self.parse_errors)

    def parse_vacancy(self, response):
        full_job_url = response.xpath('//*[@rel="canonical"]/@href').extract_first()
        # Returns job id
        job_id = response.xpath('//body/@data-id').extract_first()
        # Returns salary as a string eg. '£18,292 - £21,349 per annum, pro-rata'. ** Will need to be further processed
        salary = response.xpath('//*[@id="job-salary"]/text()').extract()
        # Returns the organization who posted it eg. 'NHS Highland'

        yield {
            'Job_Url': full_job_url,
            'Job_Id': job_id,
            'Location': salary
        }

    def parse_errors(self, failure):
        # Log all failures.
        pass
"""
This is the fourth module designed for scraping reed.co.uk.
It is a quick and dirty attempt to investigate exactly what the problems are and have a better control over things.
lxml and requests are used so that the errors can be properly explored.
It turned out to be very good and effective.

Returns:
    A dictionary of the data points specified to be collected into a MongoDB collection.
"""

from lxml import html
import requests
import time
import random
from pymongo import MongoClient


def parse_vacancy(full_job_details_page):
    """
    This function extracts the specified data points from the response object (web page)
    :param: response(web page)
    :return: A python dictionary of all data points extracted and sends it to the database.
    """

    # Xpaths that are prone to errors as a result of being Lists have been provided alternate
    # The reason the string() is not best for all is that it converts all content in an html tag to string.
    # However, the appropriate use of /text(), @content, @value gets us to the exact data point we need.

    # This xpath returns a list of urls, the actual job url is in index [1]
    try:
        full_job_url = full_job_details_page.xpath('//*[@itemprop="url"]/@content')[1]
    except IndexError:
        full_job_url = full_job_details_page.xpath('string(//*[@itemprop="url"]/@content)')

    # This xpath returns employment type as a list [Permanent, Part-Time]
    try:
        employment_type = full_job_details_page.xpath('//*[@itemprop="employmentType"]/text()')[0]
    except IndexError:
        employment_type = full_job_details_page.xpath('string(//*[@itemprop="employmentType"]/text())')
    employment_type_split = employment_type.split(",")
    job_type = employment_type_split[0]
    # Some jobs may only have [Permanent]
    try:
        job_time = employment_type_split[1]
    except IndexError:
        job_time = ''

    # This xpath returns the location i.e. nearest city eg.- [Aberdeen]
    try:
        location = full_job_details_page.xpath('//*[@itemprop="addressLocality"]/text()')[0]
    except IndexError:
        location = full_job_details_page.xpath('string(//*[@itemprop="addressLocality"]/text())')

    # This xpath returns a region if available eg. [North England].
    try:
        region = full_job_details_page.xpath('//*[@itemprop="addressRegion"]/@content')[0]
    except IndexError:
        region = full_job_details_page.xpath('string(//*[@itemprop="addressRegion"]/@content)')

    # This xpath returns the country eg.- [Scotland]
    try:
        country = full_job_details_page.xpath('//*[@id="jobCountry"]/@value')[0]
    except IndexError:
        country = full_job_details_page.xpath('string(//*[@id="jobCountry"]/@value)')

    # This xpath returns salary as a string eg. ['£18,292 - £21,349 per annum, pro-rata']. ** Will need to be further processed
    try:
        salary = full_job_details_page.xpath('//*[@itemprop="baseSalary"]/span/text()')[0]
    except IndexError:
        salary = full_job_details_page.xpath('string(//*[@itemprop="baseSalary"]/span/text())')

    # This xpath returns the organization who posted it eg. 'NHS Highland'
    try:
        posted_by = full_job_details_page.xpath('//*[@itemprop="name"]/text()')[0]
    except IndexError:
        posted_by = full_job_details_page.xpath('string(//*[@itemprop="name"]/text())')

    # This xpath returns the url of organization who posted it eg. ''https://www.reed.co.uk/jobs/nhs-highland/p45349'
    # It is the first item in the returned list of urls.
    try:
        job_poster_url = full_job_details_page.xpath('//*[@itemprop="url"]/@content')[0]
    except IndexError:
        job_poster_url = full_job_details_page.xpath('string(//*[@itemprop="url"]/@content)')

    # This xpath returns the full job-title eg. 'Senior Health & Social Care Support Worker'
    try:
        job_title = full_job_details_page.xpath('//*[@class="col-xs-12"]/h1/text()')[0]
    except IndexError:
        job_title = full_job_details_page.xpath('string(//*[@class="col-xs-12"]/h1//text())')

    # This xpath returns the date job was posted.
    try:
        date_posted = full_job_details_page.xpath('//*[@itemprop="datePosted"]/@content')[0]
    except IndexError:
        date_posted = full_job_details_page.xpath('string(//*[@itemprop="datePosted"]/@content)')

    # This xpath returns the date job is valid till.
    try:
        valid_till = full_job_details_page.xpath('//*[@itemprop="validThrough"]/@content')[0]
    except IndexError:
        valid_till = full_job_details_page.xpath('string(//*[@itemprop="validThrough"]/@content)')

    # This xpath returns the job industry
    try:
        industry = full_job_details_page.xpath('//*[@itemprop="industry"]/@content')[0]
    except IndexError:
        industry = full_job_details_page.xpath('string(//*[@itemprop="industry"]/@content)')

    # This xpath returns the full job description with html tags i.e. as an html element.
    # So the 'string()' will help to extract the text content of the tree
    job_desc = full_job_details_page.xpath('string(//*[@itemprop="description"])')

    # This xpath returns job id with some starting words that should be eliminated- "Reference: 35610799"
    try:
        job_id = full_job_details_page.xpath('//*[@class="reference text-center"]/text()')[0][11:]
    except IndexError:
        job_id = full_job_details_page.xpath('string(//*[@class="reference text-center"]/text())')
        job_id = job_id[11:]

    # Adds the time crawled
    time_crawled = time.asctime()

    # Adds the name of the data source.
    data_source = "reed.co.uk"

    scraped_data = {
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

    return scraped_data


if __name__ == "__main__":
    # execute only if run as a script
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36"}
    start_page_url = 'https://www.reed.co.uk/jobs?pageno=1&sortby=DisplayDate'
    page_no = 1
    total_no_of_jobs_so_far = 0
    sleep_time = random.randint(0, 3)
    while start_page_url is not None:
        time.sleep(sleep_time)
        start_page = requests.get(start_page_url, headers=headers)
        tree = html.fromstring(start_page.content)

        job_urls = tree.xpath('//h3/a/@href')
        job_ad_number = 0
        for job_url in job_urls:
            actual_url = 'https://www.reed.co.uk' + job_url
            time.sleep(sleep_time)
            full_page = requests.get(actual_url, headers=headers)
            string_full_page_response = html.fromstring(full_page.content)
            data = parse_vacancy(string_full_page_response)
            # Inserting into the database
            client = MongoClient('localhost', 27017)
            my_database = client['lmi']
            collection = my_database['reed-jobs']
            job_ad_inserted = collection.insert_one(data)
            job_ad_number = job_ad_number + 1
            total_no_of_jobs_so_far = total_no_of_jobs_so_far + 1
            # Logging to console for monitoring.
            print('This is job number: ' + str(job_ad_number) + ' on page: ' + str(
                page_no) + ' total no. of jobs so far: ' + str(total_no_of_jobs_so_far) + ' inserted_id: ' + str(
                job_ad_inserted.inserted_id) + ' data: ' + str(data))

        next_page_url = tree.xpath('//*[@id="nextPage"]/@href')
        for the_url in next_page_url:
            url_part_a = 'https://www.reed.co.uk/jobs?pageno='
            url_part_b = str(int(the_url.split('=')[1].split('&')[0]) + 1)
            url_part_c = '&sortby=DisplayDate'
            absolute_next_page_url = url_part_a + url_part_b + url_part_c
            start_page_url = absolute_next_page_url
            page_no = page_no + 1



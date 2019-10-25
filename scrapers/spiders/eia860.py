# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request


class Eia860Spider(scrapy.Spider):
    name = 'eia860'
    allowed_domains = ['www.eia.gov']
    start_urls = ['https://www.eia.gov/electricity/data/eia860/']

    def parse(self, response):
        pass

    def all_forms(self, response):
        """
        Produce requests for collectable Eia860 forms

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia860 ZIP files from 2001 to the most
            recent available
        """
        links = response.xpath(
            "//table[@class='simpletable']"
            "//td[2]"
            "/a[contains(text(), 'ZIP')]")

        for l in links:
            title = l.xpath('@title').extract_first().strip()
            year = int(title.split(" ")[-1])

            if year < 2001:
                continue

            url = response.urljoin(l.xpath("@href").extract_first())

            yield Request(url, meta={"year": year})

    def form_for_year(self, response, year):
        """
        Produce request for a specific Eia860 form

        Args:
            response (scrapy.http.Response): Must contain the main page
            year (int): integer year, 2001 to the most recent available

        Returns:
            Single scrapy.http.Request for Eia860 ZIP file
        """
        if year < 2001:
            raise ValueError("Years prior to 2001 not supported")

        path = "//table[@class='simpletable']//td[2]/" \
               "a[contains(@title, '%d')]/@href" % year

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(url, meta={"year": year})

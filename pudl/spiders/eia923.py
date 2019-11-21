# -*- coding: utf-8 -*-
from datetime import date
import os
import scrapy
from scrapy.http import Request

from pudl import items


class Eia923Spider(scrapy.Spider):
    name = 'eia923'
    allowed_domains = ['www.eia.gov']
    start_urls = ['https://www.eia.gov/electricity/data/eia923/']

    def __init__(self, year=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 2001:
                raise ValueError("Years before 2001 are not supported")

        self.year = year

    def parse(self, response):
        """
        Parse the eia923 home page

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            appropriate follow-up requests to collect ZIP files
        """
        if self.year is None:
            yield from self.all_forms(response)

        else:
            yield self.form_for_year(response, self.year)

    # Parsers

    def all_forms(self, response):
        """
        Produce requests for collectable Eia923 forms

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia923 ZIP files from 2001 to the most
            recent available
        """
        links = response.xpath(
            "//table[@class='simpletable'][1]"
            "//td[2]"
            "/a[contains(text(), 'ZIP')]")

        for l in links:
            title = l.xpath('@title').extract_first().strip()
            year = int(title.split(" ")[-1])

            if year < 2001:
                continue

            url = response.urljoin(l.xpath("@href").extract_first())

            yield Request(url, meta={"year": year}, callback=self.parse_form)

    def form_for_year(self, response, year):
        """
        Produce request for a specific Eia923 form

        Args:
            response (scrapy.http.Response): Must contain the main page
            year (int): integer year, 2001 to the most recent available

        Returns:
            Single scrapy.http.Request for Eia923 ZIP file
        """
        if year < 2001:
            raise ValueError("Years prior to 2001 not supported")

        path = "//table[@class='simpletable']//td[2]/" \
               "a[contains(@title, '%d')]/@href" % year

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(url, meta={"year": year}, callback=self.parse_form)

    def parse_form(self, response):
        """
        Produce the Eia923 form projects

        Args:
            response (scrapy.http.Response): Must contain the downloaded ZIP
                archive

        Yields:
            items.Eia923
        """
        path = os.path.join(
            self.settings["SAVE_DIR"], "eia923", "eia923-%d.%s.zip" %
            (response.meta["year"], date.today().isoformat()))

        yield items.Eia923(
            data=response.body, year=response.meta["year"], save_path=path)

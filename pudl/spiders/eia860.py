# -*- coding: utf-8 -*-
from datetime import date
import os
import scrapy
from scrapy.http import Request

from pudl import items


class Eia860Spider(scrapy.Spider):
    name = 'eia860'
    allowed_domains = ['www.eia.gov']
    start_urls = ['https://www.eia.gov/electricity/data/eia860/']

    def __init__(self, year=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 2001:
                raise ValueError("Years before 2001 are not supported")

        self.year = year

        # Don't try to access SETTINGS during init because scrapy does not
        # provide them on test spiders.
        self.subdir = os.path.join("eia860", date.today().isoformat())

    def parse(self, response):
        """
        Parse the eia860 home page

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

            yield Request(url, meta={"year": year}, callback=self.parse_form)

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
            return Request(url, meta={"year": year}, callback=self.parse_form)

    def parse_form(self, response):
        """
        Produce the Eia860 form projects

        Args:
            response (scrapy.http.Response): Must contain the downloaded ZIP
                archive

        Yields:
            items.Eia860
        """
        path = os.path.join(
            self.settings["SAVE_DIR"], self.subdir,
            "eia860-%s.zip" % response.meta["year"])

        yield items.Eia860(
            data=response.body, year=response.meta["year"], save_path=path)

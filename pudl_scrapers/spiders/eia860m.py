"""
Spider for EIA 860 M.

This module include th
"""
# -*- coding: utf-8 -*-
from pathlib import Path
import scrapy
from scrapy.http import Request

import pudl_scrapers


class Eia860MSpider(scrapy.Spider):
    """Spider for Monthly EIA 860."""

    name = 'eia860m'
    allowed_domains = ['www.eia.gov']

    def __init__(self, year=None, month=None, *args, **kwargs):
        """Spider for scrapping EIA 860 Monthly."""
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 2005:
                raise ValueError("Years before 2005 are not supported")

        self.year = year
        self.month = month

    def start_requests(self):
        """Finalize setup and yield the initializing request."""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / self.name
        self.output_dir = pudl_scrapers.helpers.new_output_dir(output_root)

        yield Request("https://www.eia.gov/electricity/data/eia860m/")

    def parse(self, response):
        """
        Parse the EIA 860 M main page.

        Get responses for either all EIA 860 M XLSX files, or grab a specific
        month and year (as specified in the arguments of this object).

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            appropriate follow-up requests to collect XLSX files
        """
        if self.year is None or self.month is None:
            yield from self.all_forms(response)

        else:
            yield self.form_for_month_year(response,
                                           month=self.month,
                                           year=self.year,)

    # Parsers

    def all_forms(self, response):
        """
        Produce requests for collectable EIA 860M forms.

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia860M XLSX files from 2005 to the most
            recent available
        """
        links = response.xpath(
            "//table[@class='basic-table full-width']"
            "//td[2]"
            # "/a[contains(text(), 'XLS')]"
        )

        for l in links:
            title = l.xpath('a/@title').extract_first().strip()
            # the format for the title is 'EIA 860M MONTH YEAR'
            year = int(title.split(" ")[-1])
            month = title.split(" ")[-2]
            yield self.form_for_month_year(response, month, year)

    def form_for_month_year(self, response, month, year):
        """
        Produce request for a specific EIA 860M forms.

        Args:
            response (scrapy.http.Response): Must contain the main page
            year (int): integer year, 2001 to the most recent available

        Returns:
            Single scrapy.http.Request for Eia860 XLSX file
        """
        if year < 2015:
            raise ValueError("Years prior to 2015 not supported")

        path = "//table[@class='basic-table full-width']" \
               f"//td[2]/a[contains(@title, '{month} {year}')]/@href"

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(url,
                           meta={"year": year, "month": month},
                           callback=self.parse_form)

    def parse_form(self, response):
        """
        Produce the EIA 860M form projects.

        Args:
            response (scrapy.http.Response): Must contain the downloaded XLSX
                archive

        Yields:
            pudl_scrapers.items.Eia860M
        """
        path = self.output_dir / (
            f"eia860m-{response.meta['year']}-{response.meta['month']}.xlsx"
        )

        yield pudl_scrapers.items.Eia860M(
            data=response.body, year=response.meta["year"],
            month=response.meta["month"], save_path=path
        )

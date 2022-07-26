"""
Spider for EIA 860 M.

This module include the required infromation to establish a scrapy spider for
EIA 860M. Most of the logic in here revolves around finding the right page,
finding the right links from the page and storing how and where they should be
saved. EIA 860 M is different from the other spiders in that we need the
ability to specify both a month and a year to scrape. Right now the two modes
are grab everything or grab a specific month and year combo.

"""

import logging
from pathlib import Path
from time import strptime

import scrapy
from scrapy.http import Request

import pudl_scrapers

logger = logging.getLogger(__name__)


class Eia860MSpider(scrapy.Spider):
    """Spider for Monthly EIA 860M."""

    name = "eia860m"
    allowed_domains = ["www.eia.gov"]

    def __init__(self, year=None, month=None, *args, **kwargs):
        """
        Spider for scrapping EIA 860 Monthly.

        If a year is specified a month must also be specified. Not all
        year/month combos will provide data. The months available are quite
        erradic. Most years all months are available, but in 2015 only July -
        December are available and the most recent year's month should change
        regularly. If a bad year/month combo is given nothing will be returned.
        Check for availability before scrapping:
        www.eia.gov/electricity/data/eia860m/



        Args:
            year (int): year of available EIA 860 M data. Currently only
                available from 2015 through 2020. This works only in
                conjunction with specifying a month. Default is None.
            month (string): full name of month to grab. This works only in
                conjunction with specifying a year. Default is None.
        """
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 2015:
                raise ValueError("Years before 2015 are not supported")
        # force the month to be capitalized.
        if month is not None:
            month = month.title()

        month_year_combo = [month, year]
        if not any(
            [
                all(v is None for v in month_year_combo),
                all(v is not None for v in month_year_combo),
            ]
        ):
            raise AssertionError(
                "Scrapping a specific month without a specified year is "
                "not supported."
            )

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
            yield self.form_for_month_year(
                response,
                month=self.month,
                year=self.year,
            )

    # Parsers

    def all_forms(self, response):
        """
        Produce requests for collectable EIA 860M forms.

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia860M XLSX files from 2015 to the most
            recent available
        """
        links = response.xpath("//table[@class='basic-table full-width']" "//td[2]")
        if not links:
            logger.info("No links were found for EIA 860 M.")

        for l in links:
            title = l.xpath("a/@title").extract_first().strip()
            # the format for the title is 'EIA 860M MONTH YEAR'
            year = int(title.split(" ")[-1])
            month = title.split(" ")[-2]
            yield self.form_for_month_year(response, month, year)

    def form_for_month_year(self, response, month, year):
        """
        Produce request for a specific EIA 860 M forms.

        Args:
            response (scrapy.http.Response): Must contain the main page
            month (string): title case full name of month to grab.
            year (int): integer year, 2015 to the most recent available

        Returns:
            Single scrapy.http.Request for EIA 860 M XLSX file
        """
        if year < 2015:
            raise ValueError("Years prior to 2015 not supported")

        path = (
            "//table[@class='basic-table full-width']"
            f"//td[2]/a[contains(@title, '{month} {year}')]/@href"
        )

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(
                url,
                meta={
                    "year": year,
                    "month": str(strptime(month, "%B").tm_mon).zfill(2),
                },
                callback=self.parse_form,
            )
        else:
            logger.info(f"No links were found for EIA 860M {month}, {year}.")

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
            data=response.body,
            year=response.meta["year"],
            month=response.meta["month"],
            save_path=path,
        )

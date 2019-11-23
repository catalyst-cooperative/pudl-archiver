# -*- coding: utf-8 -*-
from datetime import date
import os
import re
import scrapy
from scrapy.http import Request

from pudl import items


class IpmSpider(scrapy.Spider):
    name = "ipm"
    allowed_domains = ["www.epa.gov"]
    start_urls = ["https://www.epa.gov/airmarkets/"
                  "national-electric-energy-data-system-needs-v6"]

    def parse(self, response):
        """
        Parse the IPM NEEDS database home page

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            appropriate follow-up requests to collect IPM xlsx data
        """
        yield from self.all_forms(response)

    def all_forms(self, response):
        """
        Parse all download urls from the IPM NEEDS database home page

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            appropriate follow-up requests to collect IPM xlsx data
        """
        links = response.xpath(
            '//a[@class="file-link" and starts-with(text(), "NEEDS v")]')

        for link in links:
            description = link.xpath("text()").extract_first()
            metadata = {"version": self.needs_version(description),
                        "revision": self.needs_revision(description)}

            url = response.urljoin(link.xpath('@href').extract_first())
            yield Request(url, callback=self.parse_form, meta=metadata)

    def parse_form(self, response):
        """
        Produce the Eia860 form projects

        Args:
            response (scrapy.http.Response): Must contain the downloaded xlsx
            file

        Yields:
            items.Ipm
        """
        path = os.path.join(
            self.settings["SAVE_DIR"], "ipm", "ipm-v%d-rev_%s.%s.xlsx" %
            (response.meta["version"], response.meta["revision"].isoformat(),
            date.today()))

        yield items.Ipm(
            data=response.body, version=response.meta["version"],
            revision=response.meta["revision"], save_path=path)

    # helpers

    def needs_version(self, text):
        """
        Get the version number from a NEEDS file description

        Args:
            text: str description, eg "NEEDS v6 rev: 5-31-2019"

        Returns:
            int, version of the NEEDS file
        """
        match = re.search("^NEEDS v([\\d]+)", text)

        if match is None:
            return

        return int(match.groups()[0])

    def needs_revision(self, text):
        """
        Get the version number from a NEEDS file description

        Args:
            text: str description, eg "NEEDS v6 rev: 5-31-2019"

        Returns:
            datetime.date: the revision date
        """
        match = re.search("rev: ([\\d]+)-([\\d]+)-([\\d]+)$", text)

        if match is None:
            return

        month, day, year = match.groups()
        return date(int(year), int(month), int(day))

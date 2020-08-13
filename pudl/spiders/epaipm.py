# -*- coding: utf-8 -*-
from datetime import date
from pathlib import Path

import re
import scrapy
from scrapy.http import Request

from pudl import items
from pudl.helpers import new_output_dir


class EpaIpmSpider(scrapy.Spider):
    name = "epaipm"
    allowed_domains = ["www.epa.gov"]

    def start_requests(self):
        """Finalize setup and yield the initializing request"""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "epaipm"
        self.output_dir = new_output_dir(output_root)

        yield Request("https://www.epa.gov/airmarkets/"
                      "national-electric-energy-data-system-needs-v6")

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
            items.EpaIpm
        """
        path = self.output_dir / (
            "epaipm-v%d-rev_%s.xlsx" %
            (response.meta["version"], response.meta["revision"].isoformat()))

        yield items.EpaIpm(
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

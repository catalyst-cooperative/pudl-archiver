"""Scrapy spider for the EIA-860 data."""
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Eia860Spider(scrapy.Spider):
    """Scrapy spider for the EIA-860 data."""
    name = "eia860"
    allowed_domains = ["www.eia.gov"]

    def __init__(self, year=None, *args, **kwargs):
        """Initialize the EIA-860 Spider."""
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 2001:
                raise ValueError("Years before 2001 are not supported")

        self.year = year

    def start_requests(self):
        """Finalize setup and yield the initializing request."""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "eia860"
        self.output_dir = new_output_dir(output_root)

        yield Request("https://www.eia.gov/electricity/data/eia860/")

    def parse(self, response):
        """Parse the eia860 home page.

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
        """Produce requests for collectable Eia860 forms.

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia860 ZIP files from 2001 to the most
            recent available
        """
        links = response.xpath(
            "//table[@class='simpletable']" "//td[2]" "/a[contains(text(), 'ZIP')]"
        )

        for link in links:
            title = link.xpath("@title").extract_first().strip()
            year = int(title.split(" ")[-1])

            if year < 2001:
                continue

            url = response.urljoin(link.xpath("@href").extract_first())

            yield Request(url, meta={"year": year}, callback=self.parse_form)

    def form_for_year(self, response, year):
        """Produce request for a specific Eia860 form.

        Args:
            response (scrapy.http.Response): Must contain the main page
            year (int): integer year, 2001 to the most recent available

        Returns:
            Single scrapy.http.Request for Eia860 ZIP file
        """
        if year < 2001:
            raise ValueError("Years prior to 2001 not supported")

        path = (
            "//table[@class='simpletable']//td[2]/"
            f"a[contains(@title, '{year}')]/@href"
        )

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(url, meta={"year": year}, callback=self.parse_form)

    def parse_form(self, response):
        """Produce the Eia860 form projects.

        Args:
            response (scrapy.http.Response): Must contain the downloaded ZIP
                archive

        Yields:
            items.Eia860
        """
        path = self.output_dir / f"eia860-{response.meta['year']}.zip"

        yield items.Eia860(
            data=response.body, year=response.meta["year"], save_path=path
        )

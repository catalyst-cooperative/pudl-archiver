"""Scrapy spider for the EIA-861 data."""

from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Eia861Spider(scrapy.Spider):
    """Scrapy spider for the EIA-861 data."""

    name = "eia861"
    allowed_domains = ["www.eia.gov"]

    def __init__(self, year=None, *args, **kwargs):
        """Initialize the spider."""
        super().__init__(*args, **kwargs)

        if year is not None:
            year = int(year)

            if year < 1990:
                raise ValueError("Years before 1990 are not supported")

        self.year = year

    def start_requests(self):
        """Finalize setup and yield the initializing request."""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "eia861"
        self.output_dir = new_output_dir(output_root)

        yield Request("https://www.eia.gov/electricity/data/eia861/")

    def parse(self, response):
        """Parse the EIA-861 home page.

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
        """Produce requests for collectable EIA-861 forms.

        Args:
            response (scrapy.http.Response): Must contain the main page

        Yields:
            scrapy.http.Requests for Eia861 ZIP files from 1990 to the most
            recent available
        """
        links = response.xpath(
            "//table[@class='simpletable']" "//td[2]" "/a[contains(text(), 'ZIP')]"
        )
        # There may be duplicates for a year (different formats).
        # First, collect the set of years, then return results by year.
        # (form_for_year contains the logic to choose which version we want)
        years = set()
        for link in links:
            title = link.xpath("@title").extract_first().strip()
            year = int(title.split(" ")[-1])

            if year < 1990:
                continue
            years.add(year)
        for year in reversed(sorted(years)):
            yield self.form_for_year(response, year)

    def form_for_year(self, response, year):
        """Produce request for a specific EIA-861 form.

        Args:
            response (scrapy.http.Response): Must contain the main page
            year (int): integer year, 1990 to the most recent available

        Returns:
            Single scrapy.http.Request for Eia861 ZIP file
        """
        if year < 1990:
            raise ValueError("Years prior to 1990 not supported")

        path = (
            "//table[@class='simpletable']//td[2]/"
            f"a[contains(@title, '{year}')]/@href"
        )

        # Since April or May 2020, the EIA website has provided "original" and
        # "reformatted" versions of the data for 1990-2011. Select the
        # original data by taking the first column ('td[2]' above)

        link = response.xpath(path).extract_first()

        if link is not None:
            url = response.urljoin(link)
            return Request(url, meta={"year": year}, callback=self.parse_form)

    def parse_form(self, response):
        """Produce the EIA-861 form projects.

        Args:
            response (scrapy.http.Response): Must contain the downloaded ZIP
                archive

        Yields:
            items.Eia861
        """
        path = self.output_dir / f"eia861-{response.meta['year']}.zip"

        yield items.Eia861(
            data=response.body, year=response.meta["year"], save_path=path
        )

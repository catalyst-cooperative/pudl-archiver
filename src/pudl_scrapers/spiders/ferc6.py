"""Scrapy spider for downloading FERC Form 6 data."""
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Ferc6Spider(scrapy.Spider):
    """Scrapy spider for downloading FERC Form 6 data."""

    name = "ferc6"
    allowed_domains = ["www.ferc.gov"]
    start_urls = [
        "https://www.ferc.gov/general-information-1/oil-industry-forms/form-6-6q-historical-vfp-data"
    ]

    def start_requests(self):
        """Start requesting FERC 6 forms.

        Yields:
            List of Requests for FERC 6 forms
        """
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "ferc6"
        self.output_dir = new_output_dir(output_root)

        yield from self.all_form_requests()

    def parse(self, response):
        """Produce the FERC item.

        Args:
            response: scrapy.http.Response containing ferc6 data

        Yields:
            Ferc6 item
        """
        path = self.output_dir / f"ferc6-{response.meta['year']}.zip"

        yield items.Ferc6(
            data=response.body, year=response.meta["year"], save_path=path
        )

    def form_for_year(self, year: int):
        """Produce a form request for the given year.

        Args:
            year: Report year of the data to scrape.

        Returns:
            Request for the Ferc 6 form
        """
        url = f"https://forms.ferc.gov/f6allyears/f6_{year}.zip"
        return Request(url, meta={"year": year}, callback=self.parse)

    def all_form_requests(self):
        """Produces form requests for all supported years.

        Yields:
            Requests for all available FERC Form 6 zip files
        """
        for year in range(2000, 2022):
            yield self.form_for_year(year)

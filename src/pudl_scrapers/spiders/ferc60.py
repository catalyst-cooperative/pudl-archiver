"""Scrapy spider for downloading FERC Form 60 data."""
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Ferc60Spider(scrapy.Spider):
    """Scrapy spider for downloading FERC Form 60 data."""

    name = "ferc60"
    allowed_domains = ["www.ferc.gov"]
    start_urls = [
        "https://www.ferc.gov/filing-forms/service-companies-filing-forms/Form-60-Historical-VFP-Data"
    ]

    def start_requests(self):
        """Start requesting FERC 60 forms.

        Yields:
            List of Requests for FERC 60 forms
        """
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "ferc60"
        self.output_dir = new_output_dir(output_root)

        yield from self.all_form_requests()

    def parse(self, response):
        """Produce the FERC item.

        Args:
            response: scrapy.http.Response containing FERC Form 60 data

        Yields:
            Ferc60 item
        """
        path = self.output_dir / f"ferc6-{response.meta['year']}.zip"

        yield items.Ferc60(
            data=response.body, year=response.meta["year"], save_path=path
        )

    def form_for_year(self, year: int):
        """Produce a form request for the given year.

        Args:
            year: Report year of the data to scrape.

        Returns:
            Request for the Ferc 60 form
        """
        url = f"https://forms.ferc.gov/f60allyears/f60_{year}.zip"
        return Request(url, meta={"year": year}, callback=self.parse)

    def all_form_requests(self):
        """Produces form requests for all supported years.

        Yields:
            Requests for all available FERC Form 60 zip files
        """
        for year in range(2006, 2021):
            yield self.form_for_year(year)

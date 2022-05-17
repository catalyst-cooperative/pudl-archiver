"""
Spider for EPA-EIA Crosswalk.

This module include the required infromation to establish a scrapy spider for
the EPA-EIA Crosswalk. It pulls both the Crosswalk csv file and the field
descriptions csv.

"""

import logging
from pathlib import Path

import pudl_scrapers
import scrapy
from pudl_scrapers import items
from scrapy.http import Request


logger = logging.getLogger(__name__)


class EpaEiaSpider(scrapy.Spider):
    """Spider for EPA-EIA Crosswalk."""

    name = "epa-eia-crosswalk"
    allowed_domains = ["www.github.com/USEPA"]

    def __init__(self, year=None, *args, **kwargs):
        """Spider for scraping the EPA-EIA crosswalk."""
        super().__init__(*args, **kwargs)

    def start_requests(self):
        """Finalize setup and yield the initializing request."""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / self.name
        self.output_dir = pudl_scrapers.helpers.new_output_dir(output_root)

        urls = [
            "https://github.com/USEPA/camd-eia-crosswalk/raw/master/epa_eia_crosswalk.csv",
            "https://github.com/USEPA/camd-eia-crosswalk/raw/master/field_descriptions.csv",
        ]

        for url in urls:
            yield Request(url)

    def parse(self, response):
        """Parse the downloaded census zip file."""
        filename = response.url.split("/")[-1]
        path = self.output_dir / filename
        yield items.EpaEiaCrosswalk(data=response.body, save_path=path)

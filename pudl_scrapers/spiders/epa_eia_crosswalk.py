"""
Spider for EPA-EIA Crosswalk.

This module include the required infromation to establish a scrapy spider for
the EPA-EIA Crosswalk.

"""

import logging
from pathlib import Path

import pudl_scrapers
import scrapy
from scrapy.http import Request


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

        yield Request("https://github.com/USEPA/camd-eia-crosswalk")


    def parse(self, response):
        

"""
Spider for EPA-EIA Crosswalk.

This module include the required infromation to establish a scrapy spider for
the EPA-EIA Crosswalk. It pulls the entire repo zip file from github.

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

    def start_requests(self):
        """Finalize setup and yield the initializing request."""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / self.name
        self.output_dir = pudl_scrapers.helpers.new_output_dir(output_root)

        yield Request(
            "https://github.com/USEPA/camd-eia-crosswalk/archive/refs/heads/master.zip"
        )

    def parse(self, response):
        """Parse the downloaded census zip file."""
        path = self.output_dir / "epa_eia_crosswalk.zip"
        yield items.EpaEiaCrosswalk(data=response.body, save_path=path)

"""Spider for EPA CAMD to EIA Crosswalk.

This module include the required infromation to establish a scrapy spider for
the EPA CAMD to EIA Crosswalk. It pulls the entire repo zip file from
github.

"""

import logging
from pathlib import Path

import scrapy
from scrapy.http import Request

import pudl_scrapers
from pudl_scrapers import items

logger = logging.getLogger(__name__)


class EpaCamdEiaSpider(scrapy.Spider):
    """Spider for EPA CAMD to EIA Crosswalk."""

    name = "epacamd_eia"
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
        path = self.output_dir / "epacamd_eia.zip"
        yield items.EpaCamdEia(data=response.body, save_path=path)

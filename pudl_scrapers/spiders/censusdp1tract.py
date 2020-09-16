# -*- coding: utf-8 -*-
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class CensusDp1TractSpider(scrapy.Spider):
    name = 'censusdp1tract'
    allowed_domains = ['www2.census.gov']

    def start_requests(self):
        base = Path(self.settings.get("OUTPUT_DIR"))
        cdt_dir = base / "censusdp1tract"
        self.output_dir = new_output_dir(cdt_dir)

        yield Request("https://www2.census.gov/geo/tiger/TIGER2010DP1/"
                      "Profile-County_Tract.zip")

    def parse(self, response):
        """Parse the downloaded census zip file."""
        path = self.output_dir / "censusdp1tract-2010.zip"
        yield items.Census(data=response.body, save_path=path)

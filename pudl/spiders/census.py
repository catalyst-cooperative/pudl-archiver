# -*- coding: utf-8 -*-
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl import items
from pudl.helpers import new_output_dir


class CensusSpider(scrapy.Spider):
    name = 'census'
    allowed_domains = ['www2.census.gov']

    def start_requests(self):
        base = Path(self.settings.get("OUTPUT_DIR"))
        census_dir = base / "census"
        self.output_dir = new_output_dir(census_dir)

        yield Request("https://www2.census.gov/geo/tiger/TIGER2010DP1/"
                      "Profile-County_Tract.zip")

    def parse(self, response):
        """Parse the downloaded census zip file."""
        path = self.output_dir / "census2010.zip"
        yield items.Census(data=response.body, save_path=path)

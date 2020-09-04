# -*- coding: utf-8 -*-
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Ferc714Spider(scrapy.Spider):
    name = 'ferc714'
    allowed_domains = ['www.ferc.gov']

    def start_requests(self):
        """Initialize download of FERC form 714."""
        base = Path(self.settings.get("OUTPUT_DIR"))
        ferc_dir = base / "ferc714"
        self.output_dir = new_output_dir(ferc_dir)

        yield Request("https://www.ferc.gov/sites/default/files/2020-06/"
                      "form714-database-June-2020.zip")

    def parse(self, response):
        """Parse the downloaded FERC form 714."""
        path = str(self.output_dir / "form714.zip")
        yield items.Ferc714(data=response.body, save_path=path)

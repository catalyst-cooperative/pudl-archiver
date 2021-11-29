# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class EipInfrastructureSpider(scrapy.Spider):
    name = 'eipinfrastructure'
    allowed_domains = ['environmentalintegrity.org']


    def start_requests(self):
        """Finalize setup and yield the initializing request"""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "eipinfrastructure"
        self.output_dir = new_output_dir(output_root)

        yield Request("https://environmentalintegrity.org/download/eip-emissions-increase-database/?wpdmdl=13864&refresh=619e7743265e61637775171")

    def parse(self, response):
        """Parse the downloaded EIP Infrastructure excel file."""
        filename = response.headers["Content-Disposition"].decode("utf-8")
        update_date = filename.replace('"', '').split("%20")[-1]
        extension = update_date.split(".")[-1]
        update_date = update_date.replace(f".{extension}", "")

        update_date = datetime.strptime(update_date, "%m.%d.%Y")
        update_date = update_date.date().isoformat()

        path = str(self.output_dir / f"eipinfra_{update_date}.{extension}")
        yield items.EipInfrastructure(data=response.body, save_path=path)

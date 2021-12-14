# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class LbnlIsoQueuesSpider(scrapy.Spider):
    name = 'lbnlisoqueues'
    allowed_domains = ['emp.lbl.gov', 'eta-publications.lbl.gov']

    def start_requests(self):
        """Finalize setup and yield the initializing request"""
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "lbnlisoqueues"
        self.output_dir = new_output_dir(output_root)

        yield Request("https://emp.lbl.gov/publications/queued-characteristics-power-plants")

    def parse(self, response):
        """Parse the downloaded EIP Infrastructure excel file."""
        download_url = response.xpath("//a[@title='Queues Data File XLSX']").attrib['href']
        yield Request(download_url, callback=self.parse_form)

    def parse_form(self, response):
        # This assumes lbln updates the data and uses the filename queues_{year}_clean_data.xlsx 
        filename = response.url.split("/")[-1]
        update_date = filename.split("_")[1]

        path = str(self.output_dir / f"lbnlisoqueues_{update_date}.xlsx")
        yield items.LblnIsoQueues(data=response.body, save_path=path)

# -*- coding: utf-8 -*-
import ftplib

import os
import scrapy
from scrapy.http import Request

from pudl import items


class CemsFtpManager:
    """Custom client for the EPA CEMS ftp server"""

    server = "newftp.epa.gov"
    root = "dmdnload/emissions/hourly/monthly"

    def __init__(self):
        pass

    def _new_client(self):
        """Create a new, logged in client on the CEMS server"""
        client = ftplib.FTP(self.server)
        client.connect()
        client.login()
        return client

    def available_years(self):
        """
        List all available years

        Returns:
            list of ints for each year of data available on the site
        """
        client = self._new_client()
        years = [int(x) for x in client.nlst(self.root)]
        client.quit()
        return years

    def files_for_year(self, year):
        """
        List all files available for the given year

        Args:
            year: int, between 1995 and max(self.available_years())

        Returns:
            list of strings, ftp urls of files available for given year
        """
        directory = "%s/%d" % (self.root, year)
        client = self._new_client()
        files = ["ftp://%s/%s/%s" % (self.server, directory, f) for f in
                 client.nlst(directory)]

        client.quit()
        return files


class CemsSpider(scrapy.Spider):
    name = "cems"
    allowed_domains = ["newftp.epa.gov"]

    def __init__(self, *args, year=None, **kwargs):
        super().__init__(*args, **kwargs)

        if year is None:
            self.year = None
        else:
            self.year = int(year)

    def start_requests(self):
        """Produce Requests for CEMS data files"""

        ftp_manager = CemsFtpManager()

        if self.year is None:
            years = ftp_manager.available_years()
        else:
            years = [self.year]

        for year in years:
            urls = ftp_manager.files_for_year(year)

            for url in urls:
                yield Request(url)

    def parse(self, response):
        """
        Parse the cems ftp root

        Args:
            response (scrapy.http.Response): Must contain the root request

        Yields:
            appropriate follow-up requests to collect ZIP files
        """
        filename = os.path.basename(response.url)
        path = os.path.join(self.settings["SAVE_DIR"], "cems", filename)
        yield items.Cems(data=response.body, save_path=path)

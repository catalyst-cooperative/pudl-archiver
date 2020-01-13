#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ftplib
import os
import shutil

from pudl.helpers import new_output_dir
import pudl.settings


class EpaCemsFtpManager:
    """Custom client for the EPA CEMS ftp server"""

    server = "newftp.epa.gov"
    root = "dmdnload/emissions/hourly/monthly"

    def __init__(self, testing=False):
        """Initialize the CemsFtpManager"""
        self.testing = testing
        settings_output_dir = pudl.settings.OUTPUT_DIR
        output_root = os.path.join(settings_output_dir, "cems")
        self.output_dir = new_output_dir(output_root)

        os.makedirs(self.output_dir)

    def __enter__(self):
        """Support `with` statement by returning self"""
        self.client = self._new_client()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Support `with` statement with cleanup, as needed"""
        self.client.quit()

        if self.testing:
            shutil.rmtree(self.output_dir)

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
        years = [int(x) for x in self.client.nlst(self.root)]
        return years

    def download(self, remote_name):
        """Download an individual file."""
        _, file_name = os.path.split(remote_name)
        local_path = os.path.join(self.output_dir, file_name)
        cmd = "RETR %s" % remote_name

        with open(local_path, "wb") as f:
            self.client.retrbinary(cmd, f.write)

        return local_path

    def collect_year(self, year):
        """Download all files for a given year"""
        directory = "%s/%d" % (self.root, year)
        self.client.cwd(directory)

        count = 0
        file_names = self.client.nlst()

        for fn in file_names:
            self.download(fn)
            count += 1

        return count


if __name__ == "__main__":

    with EpaCemsFtpManager() as cftp:
        cftp.collect_year(1995)

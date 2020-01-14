#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import ftplib
import os
import shutil
import sys

from pudl.helpers import new_output_dir
import pudl.settings


logger = logging.Logger(__name__)
logger.addHandler(logging.FileHandler(pudl.settings.LOG_FILE))


class EpaCemsFtpManager:
    """Custom client for the EPA CEMS ftp server"""

    server = "newftp.epa.gov"
    root = "/dmdnload/emissions/hourly/monthly"

    def __init__(self, testing=False):
        """
        Initialize the CemsFtpManager

        Args:
            testing: Bool - indicates that the EpaCemsFtpManager is only being
                     tested, and should clean up after itself

        Returns:
            EpaCemsFtpManager

        """
        self.testing = testing
        settings_output_dir = pudl.settings.OUTPUT_DIR
        output_root = os.path.join(settings_output_dir, "cems")
        self.output_dir = new_output_dir(output_root)

        self.total_count = 0

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
        """
        Download an individual file.

        Args: remote_name, str: the name of the file on the remote server

        Returns: str, full file path to the downloaded file.

        """
        _, file_name = os.path.split(remote_name)
        local_path = os.path.join(self.output_dir, file_name)
        cmd = "RETR %s" % remote_name

        with open(local_path, "wb") as f:
            self.client.retrbinary(cmd, f.write)

        logger.debug("%s downloaded" % local_path)
        return local_path

    def collect_year(self, year):
        """
        Download all files for a given year

        Args: year, int, the year

        Returns:
            int, a count of the number of files downloaded
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        directory = "%s/%d" % (self.root, year)

        try:
            self.client.cwd(directory)
        except Exception as err:
            logger.error(
                "Failed to open remote dir %s, error %s. Year %d skipped."
                % (directory, err, year))
            return 0

        count = 0
        file_names = self.client.nlst()

        # TODO: only on testing
        file_names = file_names[:5]

        for fn in file_names:
            self.download(fn)
            count += 1
            self.total_count += 1

        return count


def get_arguments():
    """
    Parse the command line arguments

    Returns: result of the command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Download EPA CEMS data from the EPA's FTP server")
    parser.add_argument(
        "--year [4 digit year]", type=int,
        help="Limit collection to the provided year")
    parser.add_argument(
        "--verbose", default=False, action="store_const",
        const=True, help="Print progress details to stdout")
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = get_arguments()

    if args.verbose:
        logger.addHandler(logging.StreamHandler())

    year = getattr(args, "year", None)

    if year is not None:
        with EpaCemsFtpManager() as cftp:
            available = cftp.available_years()

            if year in available:
                cftp.collect_year(year)
            else:
                logger.error("Data for %d is not available" % year)

        sys.exit()

    with EpaCemsFtpManager() as cftp:

        for year in cftp.available_years():

            count = cftp.collect_year(year)
            logger.debug("Downloaded %d files for %d" % (count, year))

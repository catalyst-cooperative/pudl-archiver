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


states = ["al", "ak", "az", "ar", "ca", "co", "ct", "dc", "de", "fl", "ga",
          "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
          "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
          "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
          "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"]


class EpaCemsFtpManager:
    """Custom client for the EPA CEMS ftp server"""

    server = "newftp.epa.gov"
    root = "/dmdnload/emissions/hourly/monthly"

    def __init__(self, loglevel="DEBUG", verbose=False, testing=False):
        """
        Initialize the CemsFtpManager

        Args:
            loglevel: str, valid logger log-level
            verbose: bool, if true logging will be duplicated to stderr
            testing: bool - indicates that the EpaCemsFtpManager is only being
                     tested, and should clean up after itself

        Returns:
            EpaCemsFtpManager

        """
        self.testing = testing
        settings_output_dir = pudl.settings.OUTPUT_DIR
        output_root = os.path.join(settings_output_dir, "epacems")
        self.output_dir = new_output_dir(output_root)
        self.total_count = 0

        self.logger = logging.Logger(__name__)

        if verbose:
            self.logger.addHandler(logging.StreamHandler())

        self.logger.setLevel(loglevel)

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

    def file_year(self, filename):
        """
        Produce the year for an epa cems file name

        Args:
            filename: str, the name of the file as listed on the ftp server

        Returns:
            int, the year represented by the file, or None if it is
                unavailable.
        """
        try:
            return int(filename[:4])
        except ValueError:
            self.logger.warning("Missing year from %s" % filename)

    def file_month(self, filename):
        """
        Produce the month for an epa cems file name

        Args:
            filename: str, the name of the file as listed on the ftp server

        Returns:
            int, the month represented by the file, or None if it is
                unavailable
        """
        try:
            return int(filename[6:8])
        except ValueError:
            self.logger.warning("Missing month from %s" % filename)

    def file_state(self, filename):
        """
        Produce the state for an epa cems file name

        Args:
            filename: str, the name of the file as listed on the ftp server

        Returns:
            str, two digit state abbreviation
        """
        abbr = filename[4:6].lower()

        if abbr not in states:
            self.logger.warning("Missing state from %s, got %s" %
                                (filename, abbr))
            return

        return abbr

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

        Returns: True on success, False on failure

        """
        _, file_name = os.path.split(remote_name)
        local_path = os.path.join(self.output_dir, file_name)
        cmd = "RETR %s" % remote_name

        try:
            with open(local_path, "wb") as f:
                self.client.retrbinary(cmd, f.write)
        except Exception as err:
            self.logger.error("Failed to download %s: %s" % (file_name, err))
            return False

        self.logger.debug("%s downloaded" % local_path)
        return True

    def collect_year(self, year, month=None, state=None):
        """
        Download all files for a given year

        Args:
            year, int, the year
            month, int, limit collection to the given month
            state, str, limit the collection to the given state

        Returns:
            int, a count of the number of files downloaded
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        directory = "%s/%d" % (self.root, year)

        try:
            self.client.cwd(directory)
        except Exception as err:
            self.logger.error(
                "Failed to open remote dir %s, error %s. Year %d skipped."
                % (directory, err, year))
            return 0

        count = 0
        queue = self.client.nlst()

        if self.testing:
            queue = queue[:5]

        while queue != []:
            fn = queue.pop()

            if month is not None and month != self.file_month(fn):
                continue

            if state is not None and state != self.file_state(fn):
                continue

            success = self.download(fn)

            if success:
                count += 1
                self.total_count += 1
            else:
                queue.append(fn)

        self.logger.info("Downloaded %d files for year %d" % (count, year))
        return count


def get_arguments():
    """
    Parse the command line arguments

    Returns: result of the command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Download EPA CEMS data from the EPA's FTP server")

    parser.add_argument(
        "--year", type=int,
        help="Limit collection to the provided year")
    parser.add_argument(
        "--month", type=int,
        help="Limit collection to a given month (1-12)")
    parser.add_argument(
        "--state", type=str,
        help="Limit collection to a given state")

    parser.add_argument(
        "--loglevel", default="DEBUG", help="Set logging level")
    parser.add_argument(
        "--verbose", default=False, action="store_const",
        const=True, help="Print progress details to stdout")

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = get_arguments()
    year = getattr(args, "year", None)
    month = getattr(args, "month", None)
    state = getattr(args, "state", None)

    if state is not None:
        state = state.lower()

    if year is not None:
        with EpaCemsFtpManager(
                loglevel=args.loglevel, verbose=args.verbose) as cftp:
            available = cftp.available_years()

            if year in available:
                cftp.collect_year(year, month=month, state=state)
            else:
                cftp.logger.error("Data for %d is not available" % year)

        cftp.logger.info("Download complete: %d files" % cftp.total_count)
        sys.exit()

    with EpaCemsFtpManager(
                loglevel=args.loglevel, verbose=args.verbose) as cftp:

        for year in cftp.available_years():
            cftp.collect_year(year, month=month, state=state)

        cftp.logger.info("Download complete: %d files" % cftp.total_count)

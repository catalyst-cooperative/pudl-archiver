"""A command line interface (CLI) to archive data from an RSS feed."""
import argparse
import logging
import re
from typing import Optional
from zipfile import ZipFile

import coloredlogs
import feedparser
import requests


FERC_RSS_LINK = "https://ecollection.ferc.gov/api/rssfeed"


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Archive filings from RSS feed")
    parser.add_argument(
        "-r", "--rss-path", default=FERC_RSS_LINK, help="Specify path to RSS feed"
    )
    parser.add_argument(
        "-y", "--year", default=None, type=int, help="Specify single year for filter"
    )
    parser.add_argument(
        "-f", "--form-name", default="Form 1", help="Specify form name for filter"
    )
    parser.add_argument(
        "-p", "--period", default=None, help="Specify filing period for filter"
    )
    parser.add_argument(
        "-n", "--use-feed-name", default=False, action="store_true", help="Use filenames directly from feed (these are not consistent or descriptive, so custome names will be created by default)."
    )
    parser.add_argument(
        "--loglevel",
        help="Set log level",
        default="INFO",
    )
    parser.add_argument("--logfile", help="Path to logfile", default=None)

    return parser.parse_args()


def archive_filings(
    feed_path: str,
    form_name: str,
    filter_year: Optional[int] = None,
    filter_period: Optional[str] = None,
    use_feed_name: bool = False
):
    """
    Download filings and archive in zipfile.

    Args:
        feed_path: URL or local file path pointing to RSS feed.
        form_name: Name of form to for filter.
        filter_year: Filing year for filter.
        filter_period: Filing period for filter.
        use_feed_name: Use file name provided by feed, or create a more descriptive name.
    """
    logger = logging.getLogger("xbrl_extract")
    rss_feed = feedparser.parse(feed_path)

    year_str = filter_year if filter_year else "all-years"
    archive_path = f"ferc-{form_name.lower().replace(' ', '')}-{year_str}.zip"

    logger.info(f"Archiving filings in {archive_path}.")

    with ZipFile(archive_path, "w") as zipfile:
        # Actual link to XBRL filing is only available in inline html
        # This regex pattern will help extract the actual link
        xbrl_link_pat = re.compile('href="(.+\.(xml|xbrl))">(.+(xml|xbrl))<')  # noqa: W605

        # Loop through entries and filter
        for entry in rss_feed.entries:
            year = int(entry["ferc_year"])

            # Only filter years if a desired year was specified
            if filter_year is not None:
                if year != filter_year:
                    continue

            # Filter form name
            if entry["ferc_formname"] != form_name:
                continue

            # Filter period
            if filter_period is not None:
                if entry["ferc_period"] != filter_period:
                    continue

            # Get link then download filing
            link = xbrl_link_pat.search(entry["summary_detail"]["value"])
            filing = requests.get(link.group(1))

            # Create file name from filing metadata
            filer = entry["title"].replace(" ", "")
            year = entry["ferc_year"]
            period = entry["ferc_period"]

            # Construct file name, or use name from feed if instructed
            if use_feed_name:
                fname = link.group(2)
            else:
                fname = f"{filer}-{year}-{period}.xbrl"

            # Write to zipfile
            with zipfile.open(fname, "w") as f:
                logger.info(f"Writing {fname} to archive.")
                f.write(filing.text.encode("utf-8"))


def main():
    """CLI for archiving FERC XBRL filings from RSS feed."""
    args = parse_main()

    logger = logging.getLogger("xbrl_extract")
    logger.setLevel(args.loglevel)
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=args.loglevel, logger=logger)

    if args.logfile:
        file_logger = logging.FileHandler(args.logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)

    archive_filings(
        args.rss_path,
        args.form_name,
        filter_year=args.year,
        filter_period=args.period,
        use_feed_name=args.use_feed_name
    )

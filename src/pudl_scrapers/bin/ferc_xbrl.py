"""A command line interface (CLI) to archive data from an RSS feed."""
import argparse
import json
import logging
import re
import zipfile
from pathlib import Path

import feedparser
import requests
from feedparser import FeedParserDict

import pudl_scrapers.settings
from pudl_scrapers.helpers import new_output_dir

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

FERC_RSS_LINK = "https://ecollection.ferc.gov/api/rssfeed"
SUPPORTED_FORMS = [1, 2, 6, 60, 714]
# Actual link to XBRL filing is only available in inline html
# This regex pattern will help extract the actual link
XBRL_LINK_PATTERN = re.compile(r'href="(.+\.(xml|xbrl))">(.+(xml|xbrl))<')  # noqa: W605


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Archive filings from RSS feed")
    parser.add_argument(
        "-r",
        "--rss-path",
        default=FERC_RSS_LINK,
        help=f"Specify path to RSS feed. This can be either a URL or local path (default value is '{FERC_RSS_LINK}').",
    )
    parser.add_argument(
        "-y",
        "--years",
        default=2021,
        type=int,
        nargs="*",
        help="Specify a list of years to filter on. The year defines the report year the "
        "filing pertains to (default value is 'None', which will select all years available).",
    )
    parser.add_argument(
        "-f",
        "--form-number",
        default=1,
        type=int,
        help=f"Specify form number for filter. Allowable form numbers include: {SUPPORTED_FORMS}.",
    )
    parser.add_argument(
        "-p",
        "--period",
        default=None,
        help="Specify filing period for filter. Filing period defines the quarter the filing pertains "
        "to and expects a value of 'Q1', 'Q2', 'Q3', or 'Q4' (default value is 'None',"
        "which will select filings from all quarters).",
    )

    return parser.parse_args()


def archive_filings(
    feed_path: str,
    form_number: int,
    filter_years: list[int] | None,
    output_dir: Path,
    filter_period: str | None = None,
):
    """Download filings and archive in zipfile.

    Args:
        feed_path: URL or local file path pointing to RSS feed.
        form_number: Form number for filter.
        filter_years: Filing year for filter.
        output_dir: Directory to write archive to.
        filter_period: Filing period for filter.
    """
    rss_feed = feedparser.parse(feed_path)

    # If no filter years are specified archive all years
    # Only 2021 and 2022 are available at this time
    if not filter_years:
        filter_years = [2021, 2022]

    # Loop through requested years and create archive of available filings
    for year in filter_years:
        logger.info(f"Creating form {form_number} archive for year {year}")
        archive_year(rss_feed, form_number, year, output_dir, filter_period)


def archive_year(
    rss_feed: FeedParserDict,
    form_number: int,
    filter_year: int,
    output_dir: Path,
    filter_period: str | None = None,
):
    """Download filings and archive in zipfile.

    Args:
        rss_feed: Parsed RSS feed with filing metadata.
        form_number: Form number for filter.
        filter_year: Filing year for filter.
        output_dir: Directory to write archive to.
        filter_period: Filing period for filter.
    """
    # Form name for filter
    form_name = f"Form {form_number}"
    archive_path = output_dir / f"ferc{form_number}-{filter_year}.zip"

    logger.info(f"Archiving filings in {archive_path}.")

    # Save JSON file with metadata from RSS feed
    metadata = {}

    # Open zip file for archiving filings
    with zipfile.ZipFile(archive_path, "w") as archive:
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
            link = XBRL_LINK_PATTERN.search(entry["summary_detail"]["value"])
            filing = requests.get(link.group(1))

            # Add filing metadata
            filing_name = f"{entry['title']}{entry['ferc_period']}"
            if filing_name in metadata:
                metadata[filing_name].update({entry["id"]: entry})
            else:
                metadata[filing_name] = {entry["id"]: entry}

            # Write to zipfile
            with archive.open(f"{entry['id']}.xbrl", "w") as f:
                logger.info(f"Writing {entry['title']} to archive.")
                f.write(filing.text.encode("utf-8"))

        # Save snapshot of RSS feed
        with archive.open("rssfeed", "w") as f:
            logger.info("Writing rss feed metadata to archive.")
            f.write(json.dumps(metadata).encode("utf-8"))


def main():
    """CLI for archiving FERC XBRL filings from RSS feed."""
    args = parse_main()

    # Create output directory if it doesn't exist
    output_dir = new_output_dir(
        Path(pudl_scrapers.settings.OUTPUT_DIR) / f"ferc{args.form_number}"
    )
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # Validate CLI args
    if args.form_number not in SUPPORTED_FORMS:
        raise ValueError(f"Form number {args.form_number} is not a valid form.")

    if args.years:
        # Check if any years are out of range
        if any([year < 2021 for year in args.years]):
            raise ValueError("XBRL data is only available for 2021 forward")

    if args.period:
        if args.period not in ["Q1", "Q2", "Q3", "Q4"]:
            raise ValueError(f"Invalid filing period: {args.period}")

    archive_filings(
        feed_path=args.rss_path,
        form_number=args.form_number,
        output_dir=output_dir,
        filter_years=args.years,
        filter_period=args.period,
    )

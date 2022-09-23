"""A command line interface (CLI) to archive data from an RSS feed."""
import argparse
import datetime
import json
import logging
import re
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor as Executor
from enum import Enum
from functools import partial
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import pydantic
import requests
from arelle import Cntlr, ModelManager, ModelXbrl
from dateutil import rrule
from pydantic import BaseModel, Field, HttpUrl, root_validator, validator

import pudl_scrapers.settings
from pudl_scrapers.helpers import get_latest_directory, new_output_dir

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


XBRL_LINK_PATTERN = re.compile(r'href="(.+\.(xml|xbrl))">(.+(xml|xbrl))<')  # noqa: W605
"""Regex pattern to extrac link to XBRL filing from inline html contained in RSS feed."""

BASE_RSS_URL = "https://ecollection.ferc.gov/api/rssfeed"
"""URL to latest RSS feed.

The most recent 650 filings will be contained in this feed. All older filings can
be found in month specific feeds that can be retrieved by appending a query string
to this URL to specify the month and year desired.
"""

Year = pydantic.conint(ge=2000, le=datetime.datetime.today().year)
"""Constrained pydantic integer type with all years containing XBRL data."""


class FercForm(Enum):
    """Enum containing all supported FERC forms."""

    FORM_1 = "Form 1"
    FORM_2 = "Form 2"
    FORM_6 = "Form 6"
    FORM_60 = "Form 60"
    FORM_714 = "Form 714"

    def as_int(self):
        """Convert form to an integer for creating formatted strings."""
        match self:
            case FercForm.FORM_1:
                return 1
            case FercForm.FORM_2:
                return 2
            case FercForm.FORM_6:
                return 6
            case FercForm.FORM_60:
                return 60
            case FercForm.FORM_714:
                return 714

    @classmethod
    def from_int(cls, form_number: int) -> "FercForm":
        """Create form from an integer."""
        if form_number not in [1, 2, 6, 60, 714]:
            raise ValueError(f"{form_number} is not a valid FERC form number")

        return cls(f"Form {form_number}")


class FeedEntry(BaseModel):
    """This is a pydantic model to wrap a parsed entry from the RSS feed.

    This model does not include all fields available in the feed, but everything
    necessary for the scraping/archiving process.
    """

    entry_id: str = Field(..., alias="id")
    title: str
    download_url: HttpUrl
    published_parsed: datetime.datetime
    ferc_formname: FercForm
    ferc_year: Year
    ferc_period: str

    @root_validator(pre=True)
    def extract_url(cls, entry: dict):  # noqa: N805
        """Get download URL for inline html in feed entry."""
        link = XBRL_LINK_PATTERN.search(entry["summary_detail"]["value"])
        entry["download_url"] = link.group(1).replace(" ", "%")
        return entry

    @validator("published_parsed", pre=True)
    def parse_timestamp(cls, timestamp: time.struct_time):  # noqa: N805
        """Parse timestamp to a standard datetime object.

        The published timestamp is only available as a time.struct_time object in the
        feed. Converting to a datetime object makes it much more usable within the
        python ecosystem.
        """
        return datetime.datetime.fromtimestamp(time.mktime(tuple(timestamp)))

    def __hash__(self):
        """Implement hash so FeedEntry can be used in a set.

        Entry ID's are unique, so that's all that is needed for the hash.
        """
        return hash(f"{self.entry_id}")


FormFilings = dict[Year, set[FeedEntry]]
"""Type alias for a dictionary containing indexed filings for a single FERC form."""


def index_available_entries(
    requested_forms: list[FercForm],
) -> dict[FercForm, FormFilings]:
    """Parse all RSS feeds and index the available filings by Form number and year.

    FERC provides an RSS feed for accessing XBRL filings. However, primary RSS feed
    only contains the latest 650 filings. To access earlier filings, they also
    provide month specific feeds that contain all filings submitted for a specific
    month. This class will parse through all of these feeds and create an index of
    all filings.

    Args:
        requested_forms: List of requested forms to archive.
    """
    allowable_forms = [form.value for form in FercForm]

    # The first month with available filings is October, 2021
    feed_start_date = datetime.datetime(2021, 10, 1)

    # Get the last day of the previous month at the time of running
    feed_end_date = datetime.datetime.today().replace(day=1) - datetime.timedelta(
        days=1
    )

    # Get a list of all available month specific feeds
    rss_feeds = [
        f"{BASE_RSS_URL}?month={dt.month}&year={dt.year}"
        for dt in rrule.rrule(
            rrule.MONTHLY, dtstart=feed_start_date, until=feed_end_date
        )
    ]

    # Append feed with latest filings
    rss_feeds.append(BASE_RSS_URL)

    # Create dictionary for mapping filings to form/year
    indexed_filings = {
        FercForm.FORM_1: FormFilings(),
        FercForm.FORM_2: FormFilings(),
        FercForm.FORM_6: FormFilings(),
        FercForm.FORM_60: FormFilings(),
        FercForm.FORM_714: FormFilings(),
    }

    logger.info("Indexing filings available in all RSS feeds")
    # Loop through all feeds and index available filings
    for feed in rss_feeds:
        logger.info(f"Parsing RSS feed: {feed}")
        parsed_feed = feedparser.parse(feed)

        for entry in parsed_feed.entries:
            # Validate FERC form name
            if entry["ferc_formname"] not in allowable_forms:
                continue

            # There are a number of test filings in the feed. Skip these
            if "Test" in entry["title"]:
                continue

            parsed_entry = FeedEntry(**entry)

            # Get filings specific to FERC form and append new filing
            indexed_form = indexed_filings[parsed_entry.ferc_formname]
            if parsed_entry.ferc_year not in indexed_form:
                indexed_form[parsed_entry.ferc_year] = set()

            indexed_form[parsed_entry.ferc_year].add(parsed_entry)

    # Return indexed filings for all requested forms
    return {
        form: filings
        for form, filings in indexed_filings.items()
        if form in requested_forms
    }


def archive_taxonomy(form: FercForm, year: Year, archive: zipfile.ZipFile):
    """Download taxonomy and archive all files that comprise the taxonomy.

    XBRL taxonomies are made up of many different files. Each taxonomy has a single
    file/URL that serves as the entry point to the taxonomy, which will point to those
    other files. FERC does distribute archives of each taxonomy, which can be found here:
    https://ecollection.ferc.gov/taxonomyHistory. This, however, is not easy to
    access programmatically, so this function will download and archive the
    taxonomies manually. To do this, it uses Arelle to first parse the taxonomy.
    Arelle can then provide a list of URL's pointing to all files that make up the
    taxonomy.

    Args:
        form: Ferc form.
        year: Year of taxonomy version.
        archive: zipfile context manager.
    """
    # Get date used in entry point URL (first day of year)
    date = datetime.date(year, 1, 1)

    # Get integer form number
    form_number = form.as_int()

    logger.info(f"Archiving ferc{form_number}-{year} taxonomy")

    # Construct entry point URL
    taxonomy_entry_point = f"https://ecollection.ferc.gov/taxonomy/form{form_number}/{date.isoformat()}/form/form{form_number}/form-{form_number}_{date.isoformat()}.xsd"

    # Use Arelle to parse taxonomy
    cntlr = Cntlr.Cntlr()
    cntlr.startLogging(logFileName="logToPrint")
    model_manager = ModelManager.initialize(cntlr)
    taxonomy = ModelXbrl.load(model_manager, taxonomy_entry_point)

    # Loop through all files and save to appropriate location in archive
    for url in taxonomy.urlDocs.keys():
        url = urlparse(url)

        # There are some generic XML/XBRL files in the taxonomy that should be skipped
        if not url.netloc.endswith("ferc.gov"):
            continue

        # Download file
        file = requests.get(url.geturl())

        path = Path(url.path).relative_to("/")

        with archive.open(str(path), "w") as f:
            f.write(file.text.encode("utf-8"))


def archive_year(year: Year, filings: set[FeedEntry], form: FercForm, output_dir: Path):
    """Archive a single year of data for a desired form.

    Args:
        year: Year to archive.
        filings: Set of filings indexed from RSS feed.
        form: Ferc form.
        output_dir: Directory to save archived filings in.
    """
    # Get form number as integer
    form_number = form.as_int()

    metadata = {}
    archive_path = output_dir / f"ferc{form_number}-xbrl-{year}.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        # Archive taxonomy
        # The first version of the taxonomy was released in 2020
        if year > 2019:
            archive_taxonomy(form, year, archive)

        for filing in filings:
            # Download filing
            xbrl_file = requests.get(filing.download_url)

            # Add filing metadata
            filing_name = f"{filing.title}{filing.ferc_period}"
            if filing_name in metadata:
                metadata[filing_name].update({filing.entry_id: filing.json()})
            else:
                metadata[filing_name] = {filing.entry_id: filing.json()}

            # Write to zipfile
            with archive.open(f"{filing.entry_id}.xbrl", "w") as f:
                logger.info(f"Writing {filing.title} to form {form_number} archive.")
                f.write(xbrl_file.text.encode("utf-8"))

        # Save snapshot of RSS feed
        with archive.open("rssfeed", "w") as f:
            logger.info("Writing rss feed metadata to archive.")
            f.write(json.dumps(metadata).encode("utf-8"))

    logging.info(f"Finished scraping ferc{form_number}-{year}.")


def archive_form(
    form: FercForm, indexed_filings: FormFilings, use_latest_dir: bool = False
):
    """Create an archive for each year with filings available for the form in question.

    Args:
        form: Ferc form.
        indexed_filings: Dictionary mapping available filings to year of filing.
        use_latest_dir: Use the most recently created output directory.
    """
    # Get form number as integer
    form_number = form.as_int()

    # Create new output directory or use latest
    if use_latest_dir:
        output_dir = get_latest_directory(
            Path(pudl_scrapers.settings.OUTPUT_DIR) / f"ferc{form_number}"
        )
    else:
        # Create output directory if it doesn't exist
        output_dir = new_output_dir(
            Path(pudl_scrapers.settings.OUTPUT_DIR) / f"ferc{form_number}"
        )
        if not output_dir.exists():
            output_dir.mkdir(parents=True)

    # Loop through all years with filings available
    archive_year_form = partial(archive_year, form=form, output_dir=output_dir)
    with Executor(max_workers=5) as executor:
        [
            _
            for _ in executor.map(
                archive_year_form, indexed_filings.keys(), indexed_filings.values()
            )
        ]


def archive_filings(requested_forms: list[FercForm], use_latest_dir: bool = False):
    """Create archives for all requested forms.

    Args:
        requested_forms: List of forms to scrape.
        use_latest_dir: Use the most recently created output directory.
    """
    indexed_filings = index_available_entries(requested_forms)

    # Loop through indexed filings and archive
    for form, filings in indexed_filings.items():
        archive_form(form, filings, use_latest_dir)


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Archive filings from RSS feed")
    parser.add_argument(
        "-f",
        "--form-numbers",
        default=[1, 2, 6, 60, 714],
        type=int,
        nargs="*",
        help="Specify form numbers to archive. Allowable form numbers include: 1, 2, 6, 60, 714. By default, archives will be created for all forms.",
    )
    parser.add_argument(
        "-l",
        "--latest-output-dir",
        action="store_true",
        help="Use the most recently created output directory. This allows archives to be stored in same directory as DBF archives.",
    )

    return parser.parse_args()


def main():
    """CLI for archiving FERC XBRL filings from RSS feed."""
    args = parse_main()

    # Validate form numbers by converting to enum
    form_numbers = [FercForm.from_int(form_number) for form_number in args.form_numbers]

    archive_filings(form_numbers, args.latest_output_dir)

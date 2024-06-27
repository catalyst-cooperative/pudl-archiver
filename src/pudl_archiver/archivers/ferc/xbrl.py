"""A command line interface (CLI) to archive data from an RSS feed."""

import asyncio
import datetime
import io
import json
import logging
import re
import time
import zipfile
from collections.abc import Callable
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse
from zipfile import ZIP_DEFLATED

import aiohttp
import feedparser
from arelle import Cntlr, ModelManager, ModelXbrl
from dateutil import rrule
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator
from tqdm import tqdm

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import retry_async

logger = logging.getLogger(f"catalystcoop.{__name__}")

XBRL_LINK_PATTERN = re.compile(r'href="(.+\.(xml|xbrl))">(.+(xml|xbrl))<')  # noqa: W605
"""Regex pattern to extrac link to XBRL filing from inline html contained in RSS feed."""

TAXONOMY_URL_PATTERN = re.compile(
    r"https://ecollection\.ferc\.gov/taxonomy/form\d{1,3}/\d{4}-\d{2}-\d{2}/form/form\d{1,3}/(form-\d{1,3}_\d{4}-\d{2}-\d{2}).xsd"
)
"""Regex pattern to extract taxonomies from XBRL filings."""

BASE_RSS_URL = "https://ecollection.ferc.gov/api/rssfeed"
"""URL to latest RSS feed.
The most recent 650 filings will be contained in this feed. All older filings can
be found in month specific feeds that can be retrieved by appending a query string
to this URL to specify the month and year desired.
"""

Year = Annotated[int, Field(ge=1994, le=datetime.datetime.today().year)]
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

    @model_validator(mode="before")
    @classmethod
    def extract_url(cls, entry: dict):  # noqa: N805
        """Get download URL for inline html in feed entry."""
        link = XBRL_LINK_PATTERN.search(entry["summary_detail"]["value"])
        entry["download_url"] = link.group(1)
        return entry

    @field_validator("published_parsed", mode="before")
    @classmethod
    def parse_timestamp(cls, timestamp: time.struct_time):  # noqa: N805
        """Parse timestamp to a standard datetime object.

        The published timestamp is only available as a time.struct_time object in the
        feed. Converting to a datetime object makes it much more usable within the
        python ecosystem.
        """
        return datetime.datetime.fromtimestamp(time.mktime(tuple(timestamp)))

    def __hash__(self):
        """Implement hash so FeedEntry can be used in a set."""
        return hash(f"{self.download_url}")

    def __eq__(self, other: "FeedEntry"):
        """Implement eq so FeedEntry can be used in a set."""
        return self.download_url == other.download_url


def _taxonomy_zip_name_from_url(url: str) -> str:
    if not (match := TAXONOMY_URL_PATTERN.match(url)):
        raise RuntimeError(f"{url} does not appear to be a taxonomy url.")
    return f"{match.group(1).replace('_', '-')}.zip"


class FilingMetadata(BaseModel):
    """Combines RSS feed metadata with taxonomy referenced in filing."""

    filename: str
    rss_metadata: FeedEntry
    taxonomy_url: str
    taxonomy_zip_name: str

    @classmethod
    def from_rss_metadata(
        cls, rss_metadata: FeedEntry, filename: str, filing_data: bytes
    ) -> "FilingMetadata":
        """Construct metadata from RSS feed and filing data to extract taxonomy URL."""
        if not (match := TAXONOMY_URL_PATTERN.search(filing_data.decode().lower())):
            raise RuntimeError(
                f"Couldn't find taxonomy for filing {rss_metadata.download_url}"
            )

        taxonomy_url = match.group(0)
        return cls(
            filename=f"{rss_metadata.title}_form{rss_metadata.ferc_formname.as_int()}_{rss_metadata.ferc_period}_{round(rss_metadata.published_parsed.timestamp())}.xbrl".replace(
                " ", "_"
            ),
            rss_metadata=rss_metadata,
            taxonomy_url=taxonomy_url,
            taxonomy_zip_name=_taxonomy_zip_name_from_url(taxonomy_url),
        )


FormFilings = dict[Year, set[FeedEntry]]
"""Type alias for a dictionary containing indexed filings for a single FERC form."""


@cache
def index_available_entries() -> dict[FercForm, FormFilings]:
    """Parse all RSS feeds and index the available filings by Form number and year.

    FERC provides an RSS feed for accessing XBRL filings. However, primary RSS feed
    only contains the latest 650 filings. To access earlier filings, they also
    provide month specific feeds that contain all filings submitted for a specific
    month. This class will parse through all of these feeds and create an index of
    all filings.
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
    return indexed_filings


async def archive_taxonomies(
    taxonomies_referenced: set[str],
    form: FercForm,
    output_dir: Path,
    session: aiohttp.ClientSession,
):
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
        taxonomies_referenced: List of all taxonomies referenced by filings for year.
        form: Ferc form number.
        output_dir: Directory to save archived filings in.
        session: Async http client session.
    """
    taxonomy_versions = []
    archive_path = output_dir / f"ferc{form.as_int()}-xbrl-taxonomies.zip"
    with zipfile.ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        for taxonomy_entry_point in taxonomies_referenced:
            logger.info(f"Archiving {taxonomy_entry_point}.")

            # Use Arelle to parse taxonomy
            cntlr = Cntlr.Cntlr()
            cntlr.startLogging(logFileName="logToPrint")
            model_manager = ModelManager.initialize(cntlr)
            taxonomy = await retry_async(
                asyncio.to_thread,
                args=[ModelXbrl.load, model_manager, taxonomy_entry_point],
                retry_on=(FileNotFoundError, FileExistsError),
            )

            # Loop through all files and save to appropriate location in archive
            archive_files = set()
            taxonomy_buffer = io.BytesIO()
            with zipfile.ZipFile(
                taxonomy_buffer, "w", compression=ZIP_DEFLATED
            ) as taxonomy_archive:
                for url in taxonomy.urlDocs:
                    url_parsed = urlparse(url)

                    # There are some generic XML/XBRL files in the taxonomy that should be skipped
                    if not url_parsed.netloc.endswith("ferc.gov"):
                        continue

                    # Download file
                    response = await retry_async(session.get, args=[url])
                    response_bytes = await retry_async(response.content.read)
                    path = Path(url_parsed.path).relative_to("/")
                    archive_files.add(path)

                    with taxonomy_archive.open(str(path), "w") as f:
                        f.write(response_bytes)

            taxonomy_zip_name = _taxonomy_zip_name_from_url(taxonomy_entry_point)
            taxonomy_versions.append(taxonomy_zip_name)
            with archive.open(taxonomy_zip_name, mode="w") as taxonomy_zip:
                taxonomy_zip.write(taxonomy_buffer.getvalue())

    return ResourceInfo(
        local_path=archive_path,
        partitions={
            "taxonomy_versions": taxonomy_versions,
            "data_format": "XBRL_TAXONOMY",
        },
        layout=ZipLayout(file_paths=taxonomy_versions),
    )


async def archive_year(
    year: Year,
    filings: set[FeedEntry],
    form: FercForm,
    output_dir: Path,
    session: aiohttp.ClientSession,
) -> tuple[Path, set[str]]:
    """Archive a single year of data for a desired form.

    Args:
        year: Year to archive.
        filings: Set of filings indexed from RSS feed.
        form: Ferc form.
        output_dir: Directory to save archived filings in.
        session: Async http client session.
    """
    # Get form number as integer
    form_number = form.as_int()

    metadata = {}
    archive_path = output_dir / f"ferc{form_number}-xbrl-{year}.zip"

    # Track taxonomies referenced by all filings for archival
    taxonomies_referenced = set()

    files_in_zip = ["rssfeed"]
    with zipfile.ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        for filing in tqdm(filings, desc=f"FERC {form.value} {year} XBRL"):
            # Add filing metadata

            # Download filing
            try:
                response = await retry_async(
                    session.get,
                    args=[str(filing.download_url)],
                    kwargs={"raise_for_status": True},
                )
                response_bytes = await retry_async(response.content.read)
            except aiohttp.client_exceptions.ClientResponseError as e:
                logger.warning(
                    f"Failed to download XBRL filing {filing.title} for form{form_number}-{year}: {e.message}"
                )
                continue
            # Write to zipfile
            filename = f"{filing.title}_form{filing.ferc_formname.as_int()}_{filing.ferc_period}_{round(filing.published_parsed.timestamp())}.xbrl".replace(
                " ", "_"
            )
            filing_name = f"{filing.title}{filing.ferc_period}"
            if filing_name not in metadata:
                metadata[filing_name] = []

            filing_metadata = FilingMetadata.from_rss_metadata(
                filing, filename, response_bytes
            )
            metadata[filing_name].append(filing_metadata.model_dump())
            taxonomies_referenced.add(filing_metadata.taxonomy_url)

            with archive.open(filename, "w") as f:
                files_in_zip.append(filename)
                f.write(response_bytes)

        # Save snapshot of RSS feed
        with archive.open("rssfeed", "w") as f:
            logger.info("Writing rss feed metadata to archive.")
            f.write(json.dumps(metadata, default=str).encode("utf-8"))

    logger.info(f"Finished scraping ferc{form_number}-{year}.")

    return ResourceInfo(
        local_path=archive_path,
        partitions={
            "year": year,
            "data_format": "XBRL",
            "taxonomies_referenced": taxonomies_referenced,
        },
        layout=ZipLayout(file_paths=files_in_zip),
    )


async def archive_xbrl_for_form(
    form: FercForm,
    output_dir: Path,
    valid_year: Callable[[int], bool],
    session: aiohttp.ClientSession,
) -> list[ResourceInfo]:
    """Archive all XBRL filings and taxonomies for specified FERC form."""
    indexed_filings = index_available_entries()[form]
    filing_resources = []
    for year, filings in indexed_filings.items():
        if not valid_year(year):
            continue

        filing_resources.append(
            await archive_year(year, filings, form, output_dir, session)
        )

    taxonomies_referenced = {
        taxonomy
        for resource in filing_resources
        for taxonomy in resource.partitions["taxonomies_referenced"]
    }
    taxonomy_resource = await archive_taxonomies(
        taxonomies_referenced, form, output_dir, session
    )

    return [*filing_resources, taxonomy_resource]

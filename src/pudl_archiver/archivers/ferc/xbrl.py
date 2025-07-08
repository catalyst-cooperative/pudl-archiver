"""A command line interface (CLI) to archive data from an RSS feed."""

import asyncio
import datetime
import io
import json
import logging
import re
import zipfile
from collections import defaultdict
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse
from zipfile import ZIP_DEFLATED

import aiohttp
import feedparser
from arelle import Cntlr, ModelManager, ModelXbrl
from dateutil import rrule
from pydantic import BaseModel, Field, HttpUrl, model_validator
from tqdm import tqdm

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash, retry_async

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


def _get_rss_feeds() -> list[str]:
    """Return all FERC RSS feeds."""
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
    return rss_feeds


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

    title: str
    download_url: HttpUrl
    published_parsed: datetime.datetime
    ferc_formname: FercForm
    ferc_year: Year
    ferc_period: str

    @model_validator(mode="before")
    @classmethod
    def extract_url_timestamp(cls, entry: dict):  # noqa: N805
        """Get download URL from inline html in feed entry and parse timestamp."""
        # Get download URL
        link = XBRL_LINK_PATTERN.search(entry["summary_detail"]["value"])
        entry["download_url"] = link.group(1)

        # Get published datetime
        entry["published_parsed"] = datetime.datetime.strptime(
            entry["published"], "%a, %d %b %Y %X %z"
        ).astimezone(datetime.UTC)
        return entry

    def __hash__(self):
        """Implement hash so FeedEntry can be used in a set."""
        return hash(f"{self.download_url}")

    def __eq__(self, other: "FeedEntry"):
        """Implement eq so FeedEntry can be used in a set."""
        return self.download_url == other.download_url


class IndexedFilings(BaseModel):
    """Parse FERC provided RSS feeds and extract metadata for all filings for a specific form number."""

    filings_per_year: dict[Year, set[FeedEntry]]

    @classmethod
    def index_available_entries(cls, form: FercForm) -> "IndexedFilings":
        """Parse all RSS feeds and index the available filings by Form number and year.

        FERC provides an RSS feed for accessing XBRL filings. However, primary RSS feed
        only contains the latest 650 filings. To access earlier filings, they also
        provide month specific feeds that contain all filings submitted for a specific
        month.

        Returns:
            Dictionary mapping a year to all available filings for that year.
        """
        rss_feeds = _get_rss_feeds()
        indexed_filings = defaultdict(set)

        # Loop through all feeds and index available filings
        logger.info("Indexing filings available in all RSS feeds")
        for feed in rss_feeds:
            logger.info(f"Parsing RSS feed: {feed}")
            parsed_feed = feedparser.parse(feed)

            for entry in parsed_feed.entries:
                # Validate FERC form name
                if entry["ferc_formname"] != form.value:
                    continue

                # There are a number of test filings in the feed. Skip these
                if "Test" in entry["title"]:
                    continue

                parsed_entry = FeedEntry(**entry)

                # Get filings specific to FERC form and append new filing
                indexed_filings[parsed_entry.ferc_year].add(parsed_entry)

        # Sort filings by download URL for determenistic ordering
        return IndexedFilings(
            filings_per_year={
                year: sorted(year_filings, key=lambda f: f.download_url)
                for year, year_filings in indexed_filings.items()
            }
        )


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
            filename=filename,
            rss_metadata=rss_metadata,
            taxonomy_url=taxonomy_url,
            taxonomy_zip_name=_taxonomy_zip_name_from_url(taxonomy_url),
        )


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
        for taxonomy_entry_point in sorted(taxonomies_referenced):
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

                    add_to_archive_stable_hash(
                        archive=taxonomy_archive, filename=path, data=response_bytes
                    )

            taxonomy_zip_name = _taxonomy_zip_name_from_url(taxonomy_entry_point)
            taxonomy_versions.append(taxonomy_zip_name)
            add_to_archive_stable_hash(
                archive=archive,
                filename=taxonomy_zip_name,
                data=taxonomy_buffer.getvalue(),
            )

    return ResourceInfo(
        local_path=archive_path,
        partitions={
            "taxonomy_versions": taxonomy_versions,
            "data_format": "XBRL_TAXONOMY",
        },
        layout=ZipLayout(file_paths=taxonomy_versions),
    )


async def _download_filing(
    filing: FeedEntry,
    session: aiohttp.ClientSession,
) -> bytes:
    """Download a single filing."""
    # Download filing
    response = await retry_async(
        session.get,
        args=[str(filing.download_url)],
        kwargs={"raise_for_status": True},
    )
    return await retry_async(response.content.read)


async def _download_filings(
    archive: zipfile.ZipFile,
    year: Year,
    filings: set[FeedEntry],
    form: FercForm,
    session: aiohttp.ClientSession,
):
    """Download all filings for a single year/form.

    Args:
        archive: ZipFile to write filings to.
        year: Year to archive.
        filings: Set of filings indexed from RSS feed.
        form: Ferc form.
        session: Async http client session.
    """
    metadata = defaultdict(list)
    for filing in tqdm(
        sorted(filings, key=lambda f: f.download_url),
        desc=f"FERC {form.value} {year} XBRL",
    ):
        # Download filing
        response_bytes = await _download_filing(filing, session)

        # Write to zipfile
        filename = f"{filing.title}_form{filing.ferc_formname.as_int()}_{filing.ferc_period}_{round(filing.published_parsed.timestamp())}.xbrl".replace(
            " ", "_"
        )
        filing_name = f"{filing.title}{filing.ferc_period}"
        filing_metadata = FilingMetadata.from_rss_metadata(
            filing, filename, response_bytes
        )
        metadata[filing_name].append(filing_metadata.model_dump())

        add_to_archive_stable_hash(
            archive=archive, filename=filename, data=response_bytes
        )
    return {
        filename: sorted(
            metadata[filename],
            key=lambda filing: filing["rss_metadata"]["download_url"],
        )
        for filename in sorted(metadata)
    }


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

    archive_path = output_dir / f"ferc{form_number}-xbrl-{year}.zip"

    with zipfile.ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        metadata = await _download_filings(archive, year, filings, form, session)

        # Save snapshot of RSS feed
        logger.info("Writing rss feed metadata to archive.")
        add_to_archive_stable_hash(
            archive=archive,
            filename="rssfeed",
            data=json.dumps(
                metadata,
                default=str,
                indent=2,
            ).encode("utf-8"),
        )

    logger.info(f"Finished scraping ferc{form_number}-{year}.")

    # Extract list of files in archive and list of taxonomies referenced by filings
    files_in_zip = ["rssfeed"] + [
        filing_metadata["filename"]
        for filing_list in metadata.values()
        for filing_metadata in filing_list
    ]
    taxonomies_referenced = [
        filing_metadata["taxonomy_url"]
        for filing_list in metadata.values()
        for filing_metadata in filing_list
    ]

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
    indexed_filings = IndexedFilings.index_available_entries(form)
    filing_resources = []
    for year, filings in indexed_filings.filings_per_year.items():
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

"""Defines base class for archiver."""
import asyncio
import io
import logging
import tempfile
import typing
import zipfile
from abc import ABC, abstractmethod
from collections import namedtuple
from html.parser import HTMLParser
from pathlib import Path

import aiohttp

ResourceInfo = namedtuple("ResourceInfo", ["local_path", "partitions"])
"""Tuple to wrap info about downloaded resource."""

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
}

ArchiveAwaitable = typing.Generator[typing.Awaitable[ResourceInfo], None, None]
"""Return type of method get_resources.

The method get_resources should yield an awaitable that returns a ResourceInfo named tuple.
The awaitable should be an `async` function that will download a resource, then return the
ResourceInfo. This contains a path to the downloaded resource, and the working partitions
pertaining to the resource.
"""


class _HyperlinkExtractor(HTMLParser):
    """Minimal HTML parser to extract hyperlinks from a webpage."""

    def __init__(self):
        """Construct parser."""
        self.hyperlinks = set()
        super().__init__()

    def handle_starttag(self, tag, attrs):
        """Filter hyperlink tags and return href attribute."""
        if tag == "a":
            for attr, val in attrs:
                if attr == "href":
                    self.hyperlinks.add(val)


class AbstractDatasetArchiver(ABC):
    """An abstract base archiver class."""

    from pudl_archiver.zenodo.api_client import ZenodoDepositionInterface

    name: str

    def __init__(
        self, session: aiohttp.ClientSession, deposition: ZenodoDepositionInterface
    ):
        """Initialize Archiver object.

        Args:
            session: Async HTTP client session manager.
            deposition: Interface to Zenodo deposition relevant to data source.
        """
        self.session = session
        self.deposition = deposition

        # Create a temporary directory for downloading data
        self._download_directory_manager = tempfile.TemporaryDirectory()
        self.download_directory = Path(self._download_directory_manager.name)

        # Create logger
        self.logger = logging.getLogger(f"catalystcoop.{__name__}")
        self.logger.info(f"Archiving {self.name}")

    @abstractmethod
    async def get_resources(self) -> ArchiveAwaitable:
        """Abstract method that each data source must implement to download all resources.

        This method should be a generator that yields awaitable objects that will download
        a single resource and return the path to that resource, and a dictionary of its
        partitions. What this means in practice is calling an `async` function and yielding
        the results without awaiting them. This allows the base class to gather all of these
        awaitables and download the resources concurrently.
        """
        ...

    async def download_zipfile(
        self, url: str, file: Path | io.BytesIO, retries: int = 5, **kwargs
    ):
        """Attempt to download a zipfile and retry if zipfile is invalid.

        Args:
            url: URL of zipfile.
            file: Local path to write file to disk or bytes object to save file in memory.
            retries: Number of times to attempt to download a zipfile.
            kwargs: Key word args to pass to request.
        """
        for _ in range(0, retries):
            await self.download_file(url, file, **kwargs)

            if zipfile.is_zipfile(file):
                return None

        # If it makes it here that means it couldn't download a valid zipfile
        raise RuntimeError(f"Failed to download valid zipfile from {url}")

    async def download_file(self, url: str, file: Path | io.BytesIO, **kwargs):
        """Download a file using async session manager.

        Args:
            url: URL to file to download.
            file: Local path to write file to disk or bytes object to save file in memory.
            kwargs: Key word args to pass to request.
        """
        async with self.session.get(url, **kwargs) as response:
            if isinstance(file, Path):
                with open(file, "wb") as f:
                    f.write(await response.read())
            elif isinstance(file, io.BytesIO):
                file.write(await response.read())

    async def get_hyperlinks(
        self,
        url: str,
        filter_pattern: typing.Pattern | None = None,
        verify: bool = True,
    ) -> list[str]:
        """Return all hyperlinks from a specific web page.

        This is a helper function to perform very basic web-scraping functionality.
        It extracts all hyperlinks from a web page, and returns those that match
        a specified pattern. This means it can find all hyperlinks that look like
        a download link to a single data resource.

        Args:
            url: URL of web page.
            filter_pattern: If present, only return links that contain pattern.
            verify: Verify ssl certificate (EPACEMS https source has bad certificate).
        """
        # Parse web page to get all hyperlinks
        parser = _HyperlinkExtractor()
        async with self.session.get(url, ssl=verify) as response:
            text = await response.text()
            parser.feed(text)

        # Filter to those that match filter_pattern
        hyperlinks = parser.hyperlinks
        if filter_pattern:
            hyperlinks = {link for link in hyperlinks if filter_pattern.search(link)}

        # Warn if no links are found
        if not hyperlinks:
            self.logger.warning(f"No links found matching pattern from link: {url}")

        return hyperlinks

    async def create_archive(self):
        """Download all resources and create an archive for upload.

        This method uses the awaitables returned by `get_resources`. It
        coordinates downloading all resources concurrently, then creating a
        new zenodo deposition version containing those resources.
        """
        # Get all awaitables from get_resources
        resources = [resource async for resource in self.get_resources()]

        # Download resources concurrently and prepare metadata
        resource_dict = {}
        for resource_coroutine in asyncio.as_completed(resources):
            resource_info = await resource_coroutine
            self.logger.info(f"Downloaded {resource_info.local_path}.")
            resource_dict[str(resource_info.local_path.name)] = resource_info

        # Add to zenodo deposition
        await self.deposition.add_files(resource_dict)

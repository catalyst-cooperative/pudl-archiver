"""Defines base class for archiver."""
import asyncio
import datetime
import io
import logging
import tempfile
import typing
import zipfile
from abc import ABC, abstractmethod
from html.parser import HTMLParser
from pathlib import Path

import aiohttp

from pudl_scrapers.frictionless import ResourceInfo
from pudl_scrapers.zenodo.api_client import ZenodoDepositionInterface

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
}

ArchiveAwaitable = typing.Generator[typing.Awaitable[tuple[Path, dict]], None, None]


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

    name: str

    def __init__(
        self, session: aiohttp.ClientSession, depostion: ZenodoDepositionInterface
    ):
        """Initialize Archiver object."""
        self.session = session
        self.deposition = depostion

        # Create a temporary directory for downloading
        self._download_directory_manager = tempfile.TemporaryDirectory()
        self.download_directory = Path(self._download_directory_manager.name)

        # Create logger
        self.logger = logging.getLogger(f"catalystcoop.{__name__}")
        self.logger.info(f"Archiving {self.name}")

    @abstractmethod
    async def get_resources(
        self,
    ) -> ArchiveAwaitable:
        ...

    async def download_zipfile(
        self, url: str, file: Path | io.BytesIO, retries: int = 5, **kwargs
    ):
        """File to download a zipfile and retry if zipfile is invalid."""
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
        """
        async with self.session.get(url, **kwargs) as response:
            if isinstance(file, Path):
                with open(file, "wb") as f:
                    f.write(await response.read())
            elif isinstance(file, io.BytesIO):
                file.write(await response.read())

    async def get_hyperlinks(
        self, url: str, filter_pattern: typing.Pattern | None = None
    ) -> list[str]:
        """Return all hyperlinks from a specific web page.

        Args:
            url: URL of web page.
            filter_pattern: If present, only return links that contain pattern.
        """
        parser = _HyperlinkExtractor()
        async with self.session.get(url) as response:
            text = await response.text()
            parser.feed(text)

        hyperlinks = parser.hyperlinks
        if filter_pattern:
            hyperlinks = {link for link in hyperlinks if filter_pattern.search(link)}

        return hyperlinks

    def current_year(self) -> int:
        """Helper function to get the current year at run-time."""
        return datetime.datetime.today().year

    async def create_archive(self):
        """Download all resources and create an archive for upload."""
        resources = [resource async for resource in self.get_resources()]
        resource_info = {}

        for resource_coroutine in asyncio.as_completed(resources):
            resource_path, partitions = await resource_coroutine
            self.logger.info(
                f"Downloaded {resource_path} adding resource to zenodo deposition."
            )
            resource_info[str(resource_path.name)] = ResourceInfo(
                local_path=resource_path, partitions=partitions
            )

        await self.deposition.add_files(resource_info)

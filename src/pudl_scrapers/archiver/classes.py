"""Defines base class for archiver."""
import asyncio
import datetime
import logging
import tempfile
import typing
import zipfile
from abc import ABC, abstractmethod
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
    def get_resources(
        self,
    ) -> ArchiveAwaitable:
        ...

    async def download_zipfile(self, url: str, download_path: Path, retries: int = 5):
        """File to download a zipfile and retry if zipfile is invalid."""
        for _ in range(0, retries + 1):
            await self.download_file(url, download_path, encoding="utf-8")

            if zipfile.is_zipfile(download_path):
                return None

        # If it makes it here that means it couldn't download a valid zipfile
        raise RuntimeError(f"Failed to download valid zipfile from {url}")

    async def download_file(
        self, url: str, filename: Path, encoding: str | None = None
    ):
        """Download a file using async session manager."""
        async with self.session.get(url) as response:
            with open(filename, "wb") as f:
                f.write(await response.read())

    def current_year(self) -> int:
        """Helper function to get the current year at run-time."""
        return datetime.datetime.today().year

    async def create_archive(self):
        """Download all resources and create an archive for upload."""
        resources = [resource for resource in self.get_resources()]
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

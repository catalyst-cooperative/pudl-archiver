"""Defines base class for archiver."""
import asyncio
import datetime
import typing
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path

import aiohttp

from pudl_scrapers.zenodo.api_client import ZenodoDepositionInterface

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
}


class AbstractDatasetArchiver(ABC):
    """An abstract base archiver class."""

    name: str

    def __init__(
        self, session: aiohttp.ClientSession, depostion: ZenodoDepositionInterface
    ):
        """Initialize Archiver object."""
        self.session = session
        self.deposition = depostion

    @abstractmethod
    async def get_resources(
        self,
    ) -> typing.Genrator[typing.Awaitable[tuple[Path, dict]]]:
        ...

    async def download_zipfile(self, url: str, download_path: Path, retries: int = 0):
        """File to download a zipfile and retry if zipfile is invalid."""
        for _ in range(0, retries + 1):
            with open(download_path, "wb") as f:
                self.download_file(url, f, encoding="utf-8")

            if zipfile.is_zipfile(download_path):
                return None

        raise RuntimeError(f"Failed to download valid zipfile from {url}")

    async def download_file(
        self, url: str, file: typing.IO, encoding: str | None = None
    ):
        """Download a file using async session manager."""
        async with self.session.get(url) as response:
            file.write(await response.text(encoding))

    def current_year(self) -> int:
        """Helper function to get the current year at run-time."""
        return datetime.datetime.today().year

    async def create_archive(self):
        """Download all resources and create an archive for upload."""
        resources = [resource for resource in self.get_resources()]

        for resource_path, partitions in asyncio.as_completed(resources):
            await self.deposition.add_file(resource_path, partitions)

        await self.deposition.finish()

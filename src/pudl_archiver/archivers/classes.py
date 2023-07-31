"""Defines base class for archiver."""
import asyncio
import io
import logging
import tempfile
import typing
import zipfile
from abc import ABC, abstractmethod
from html.parser import HTMLParser
from pathlib import Path

import aiohttp

from pudl_archiver.archivers.validate import RunSummary, ValidationTestResult
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import retry_async

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

    name: str
    # Configurable option to run _check_missing_files during archive validation
    check_missing_files: bool = True

    def __init__(
        self, session: aiohttp.ClientSession, only_years: list[int] | None = None
    ):
        """Initialize Archiver object.

        Args:
            session: Async HTTP client session manager.
        """
        self.session = session

        # Create a temporary directory for downloading data
        self._download_directory_manager = tempfile.TemporaryDirectory()
        self.download_directory = Path(self._download_directory_manager.name)
        if only_years is None:
            only_years = []
        self.only_years = only_years

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

    async def download_file(
        self,
        url: str,
        file: Path | io.BytesIO,
        **kwargs,
    ):
        """Download a file using async session manager.

        Args:
            url: URL to file to download.
            file: Local path to write file to disk or bytes object to save file in memory.
            kwargs: Key word args to pass to request.
        """
        response = await retry_async(self.session.get, args=[url], kwargs=kwargs)
        response_bytes = await retry_async(response.read)

        if isinstance(file, Path):
            with open(file, "wb") as f:
                f.write(response_bytes)
        elif isinstance(file, io.BytesIO):
            file.write(response_bytes)

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

        response = await retry_async(
            self.session.get, args=[url], kwargs={"ssl": verify}
        )
        text = await retry_async(response.text)
        parser.feed(text)

        # Filter to those that match filter_pattern
        hyperlinks = parser.hyperlinks
        if filter_pattern:
            hyperlinks = {link for link in hyperlinks if filter_pattern.search(link)}

        # Warn if no links are found
        if not hyperlinks:
            self.logger.warning(
                f"The archiver couldn't find any hyperlinks that match {filter_pattern}."
                f"Make sure your filter_pattern is correct or if the structure of the {url} page changed."
            )

        return hyperlinks

    def _check_missing_files(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
    ) -> ValidationTestResult:
        """Check for any files from previous archive version missing in new version."""
        baseline_resources = set()
        if baseline_datapackage is not None:
            baseline_resources = {
                resource.name for resource in baseline_datapackage.resources
            }

        new_resources = {resource.name for resource in new_datapackage.resources}

        # Check for any files only in baseline_datapackage
        missing_files = baseline_resources - new_resources

        note = None
        if len(missing_files) > 0:
            note = f"The following files would be deleted by new archive version: {missing_files}"

        return ValidationTestResult(
            name="Missing file test",
            description="Check for files from previous version of archive that would be deleted by the new version",
            success=len(missing_files) == 0,
            note=note,
        )

    def generate_summary(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
    ) -> RunSummary:
        """Run a series of validation tests for a new archive, and return results.

        Args:
            baseline_datapackage: DataPackage descriptor from previous version of archive.
            new_datapackage: DataPackage descriptor from newly generated archive.
            resources: Dictionary mapping resource name to ResourceInfo.

        Returns:
            Bool indicating whether or not all tests passed.
        """
        validations = []
        if self.check_missing_files:
            validations.append(
                self._check_missing_files(baseline_datapackage, new_datapackage)
            )

        validations += self.dataset_validate_archive(
            baseline_datapackage, new_datapackage, resources
        )

        return RunSummary.create_summary(
            self.name, baseline_datapackage, new_datapackage, validations
        )

    def dataset_validate_archive(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
    ) -> list[ValidationTestResult]:
        """Hook to add archive validation tests specific to each dataset."""
        return []

    def valid_year(self, year: int | str):
        return (not self.only_years) or int(year) in self.only_years

    async def download_all_resources(self) -> dict[str, ResourceInfo]:
        """Download all resources.

        This method uses the awaitables returned by `get_resources`. It
        coordinates downloading all resources concurrently.
        """
        # Get all awaitables from get_resources
        resources = [resource async for resource in self.get_resources()]

        # Download resources concurrently and prepare metadata
        resource_dict = {}
        for resource_coroutine in asyncio.as_completed(resources):
            resource_info = await resource_coroutine
            self.logger.info(f"Downloaded {resource_info.local_path}.")
            resource_dict[str(resource_info.local_path.name)] = resource_info

        return resource_dict

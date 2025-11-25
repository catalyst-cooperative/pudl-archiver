"""Defines base class for archiver."""

import asyncio
import io
import json
import logging
import math
import tempfile
import typing
import zipfile
from abc import ABC, abstractmethod
from contextlib import nullcontext
from html.parser import HTMLParser
from pathlib import Path
from secrets import randbelow
from typing import Any

import aiohttp
import bs4
import pandas as pd
from playwright.async_api import Browser as PlaywrightBrowser
from playwright.async_api import Error as PlaywrightError

from pudl_archiver.archivers import validate
from pudl_archiver.frictionless import DataPackage, Partitions, ResourceInfo
from pudl_archiver.utils import (
    add_to_archive_stable_hash,
    retry_async,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
}
VALID_PARTITION_RANGES: dict[str, str] = {"year_quarter": "QS", "year_month": "MS"}

USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
]
"""User agents compiled from https://www.useragents.me/ in May 2025. Will need to be
updated periodically."""

ArchiveAwaitable = typing.AsyncGenerator[
    typing.Awaitable[ResourceInfo | list[ResourceInfo]]
]
"""Return type of method get_resources.

The method get_resources should yield an awaitable that returns a ResourceInfo class.
The awaitable should be an `async` function that will download a resource, then return the
ResourceInfo. This contains a path to the downloaded resource, and the working partitions
pertaining to the resource.
"""


class _HyperlinkExtractor(HTMLParser):
    """Minimal HTML parser to extract hyperlinks from a webpage."""

    def __init__(self):
        """Construct parser."""
        self.hyperlinks = {}
        self.current_hyperlink = None
        super().__init__()

    def handle_starttag(self, tag, attrs):
        """Filter hyperlink tags and return href attribute."""
        url_attrs = ["href", "src", "action", "data-url", "poster"]

        for attr, val in attrs:
            if attr in url_attrs:
                self.current_hyperlink = val
                if self.current_hyperlink not in self.hyperlinks:
                    # By default, set the name identical to the hyperlink
                    self.hyperlinks[self.current_hyperlink] = self.current_hyperlink
                break

    def handle_data(self, data):
        """Capture text content and associate it with the current URL."""
        if self.current_hyperlink and data.strip():
            # If data is present, replace the hyperlink name
            self.hyperlinks[self.current_hyperlink] = data.strip()

    def handle_endtag(self, tag):
        """Reset the current URL after processing the content of the tag."""
        self.current_hyperlink = None


async def _download_file(
    session: aiohttp.ClientSession,
    url: str,
    file: Path | io.BytesIO,
    post: bool = False,
    **kwargs,
):
    method = session.post if post else session.get
    async with method(url, **kwargs) as response:
        with file.open("wb") if isinstance(file, Path) else nullcontext(file) as f:
            async for chunk in response.content.iter_chunked(1024):
                f.write(chunk)
        return response.status


class AbstractDatasetArchiver(ABC):
    """An abstract base archiver class."""

    name: str
    concurrency_limit: int | None = None
    directory_per_resource_chunk: bool = False

    # Configure which generic validation tests to run
    fail_on_missing_files: bool = True
    fail_on_empty_invalid_files: bool = True
    fail_on_file_size_change: bool = True
    allowed_file_rel_diff: float = 0.25
    fail_on_dataset_size_change: bool = True
    allowed_dataset_rel_diff: float = 0.15
    fail_on_data_continuity: bool = True
    ignore_file_size_diff_partitions: [dict[str, Any]] = []

    def __init__(
        self,
        session: aiohttp.ClientSession,
        only_years: list[int] | None = None,
    ):
        """Initialize Archiver object.

        Args:
            session: Async HTTP client session manager.
            only_years: a list of years to download data for. If empty list or
                None, download all years' data.
            download_directory: where to save data to. Defaults to temp dir.
        """
        self.session = session

        # Create a temporary directory for downloading data
        self.download_directory_manager = tempfile.TemporaryDirectory()
        self.download_directory = Path(self.download_directory_manager.name)

        if only_years is None:
            only_years = []
        self.only_years = only_years
        self.file_validations: list[validate.FileUniversalValidation] = []

        self.failed_partitions: dict[str, Partitions] = {}

        # Create logger
        self.logger = logging.getLogger(f"catalystcoop.{__name__}")
        self.logger.info(f"Archiving {self.name}")

    async def get_soup(self, url: str) -> bs4.BeautifulSoup:
        """Get a BeautifulSoup instance for a URL using our existing session."""
        response = await retry_async(self.session.get, args=[url])
        response.raise_for_status()  # Raise for status codes not caught in retry.
        # TODO 2025-02-03: for some reason, lxml fails to grab the closing div
        # tag for tab content - so we use html.parser, which is slower.
        return bs4.BeautifulSoup(await response.text(), "html.parser")

    @abstractmethod
    def get_resources(self) -> ArchiveAwaitable | tuple[ArchiveAwaitable, Partitions]:
        """Abstract method that each data source must implement to download all resources.

        This method should be a generator that yields awaitable objects that will download
        a single resource and return the path to that resource, and a dictionary of its
        partitions. What this means in practice is calling an `async` function and yielding
        the results without awaiting them. This allows the base class to gather all of these
        awaitables and download the resources concurrently.

        While this method is defined without the `async` keyword, the
        overriding methods should be `async`.

        This is because, if there's no `yield` in the method body, static type
        analysis doesn't know that `async def ...` should return
        `Generator[Awaitable[ResourceInfo],...]` vs.
        `Coroutine[Generator[Awaitable[ResourceInfo]],...]`

        See
        https://stackoverflow.com/questions/68905848/how-to-correctly-specify-type-hints-with-asyncgenerator-and-asynccontextmanager
        for details.
        """
        ...

    async def download_zipfile_via_playwright(
        self,
        playwright_browser: PlaywrightBrowser,
        url: str,
        zip_path: Path,
    ):
        """Attempt to download a zipfile using playwright and fail if zipfile is invalid.

        Args:
            playwright_browser: async browser instance to use for fetching the URL.
            url: URL of zipfile.
            zip_path: Local path to write file to disk.
        """
        await self.download_file_via_playwright(playwright_browser, url, zip_path)

        if zipfile.is_zipfile(zip_path):
            return

        # If it makes it here that means it couldn't download a valid zipfile
        with Path.open(zip_path) as f:
            raise RuntimeError(
                f"Failed to download valid zipfile from {url}. File head: {f.read(128).lower().strip()}"
            )

    async def download_zipfile(
        self, url: str, zip_path: Path | io.BytesIO, retries: int = 5, **kwargs
    ):
        """Attempt to download a zipfile and retry if zipfile is invalid.

        Args:
            url: URL of zipfile.
            zip_path: Local path to write file to disk or bytes object to save file in memory.
            retries: Number of times to attempt to download a zipfile.
            kwargs: Key word args to pass to request.
        """
        for _ in range(0, retries):
            await self.download_file(url, zip_path, **kwargs)

            if zipfile.is_zipfile(zip_path):
                return

        # If it makes it here that means it couldn't download a valid zipfile
        with Path.open(zip_path) as f:
            raise RuntimeError(
                f"Failed to download valid zipfile from {url}. File head: {f.read(128).lower().strip()}"
            )

    async def download_file_via_playwright(
        self, playwright_browser: PlaywrightBrowser, url: str, file_path: Path
    ) -> int:
        """Download a file using playwright.

        Args:
            playwright_browser: async browser instance to use for fetching the URL.
            url: URL to file to download.
            file_path: Local path to write file to disk.
        """
        page = await playwright_browser.new_page()

        # timeout: 10 minutes, same as we use for the aiohttp session
        async with page.expect_download(timeout=10 * 60 * 1000) as download_info:
            try:
                # page.goto within a page.expect_download context always generates
                # an error with message "Page.goto: Download is starting" and a call
                # log. All evidence suggests this error is harmless and does not
                # affect the downloaded file. See also:
                # https://github.com/microsoft/playwright/issues/18430#issuecomment-1309638711
                # https://stackoverflow.com/questions/73652378/download-files-with-goto-in-playwright-python/74144570#74144570
                await page.goto(url, timeout=10 * 60 * 1000)
            except PlaywrightError as e:
                # ...but we're going to check our assumptions just in case:
                if not e.message.startswith("Page.goto: Download is starting"):
                    raise e
        download = await download_info.value
        # [2025 km] NB: playwright.download.save_as can't save to a BytesIO
        await download.save_as(file_path)
        await page.close()

    async def download_file(
        self, url: str, file_path: Path | io.BytesIO, post: bool = False, **kwargs
    ) -> int:
        """Download a file using async session manager.

        Args:
            url: URL to file to download.
            file_path: Local path to write file to disk or bytes object to save file in memory.
            kwargs: Key word args to pass to retry_async.

        Returns: status of the HTTP response written to file_path
        """
        return await retry_async(
            _download_file, [self.session, url, file_path, post], kwargs
        )

    async def download_and_zip_file(
        self, url: str, filename: str, zip_path: Path, **kwargs
    ):
        """Download and zip a file using async session manager.

        Args:
            url: URL to file to download.
            filename: name of file to be zipped
            zip_path: Local path to write file to disk.
            kwargs: Key word args to pass to retry_async.
        """
        response = await retry_async(self.session.get, args=[url], kwargs=kwargs)
        response_bytes = await retry_async(response.read)

        # Write to zipfile
        with zipfile.ZipFile(
            zip_path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            add_to_archive_stable_hash(
                archive=archive, filename=filename, data=response_bytes
            )

    def add_to_archive(self, zip_path: Path, filename: str, blob: typing.BinaryIO):
        """Add a file to a ZIP archive.

        Args:
            zip_path: path to target archive.
            filename: name of the file *within* the archive.
            blob: the content you'd like to write to the archive.
        """
        with zipfile.ZipFile(
            zip_path,
            "a",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            add_to_archive_stable_hash(
                archive=archive, filename=filename, data=blob.read()
            )

    async def download_add_to_archive_and_unlink(
        self, url: str, filename: str, zip_path: Path
    ):
        """Download a file, add it to an zip file in and archive and unlink.

        Little helper function that combines three common steps often repeated together:
        * :meth:`download_file`
        * :meth:`add_to_archive`
        * :meth:`Path.unlink`
        """
        download_path = self.download_directory / filename
        await self.download_file(url, download_path)
        self.add_to_archive(
            zip_path=zip_path,
            filename=filename,
            blob=download_path.open("rb"),
        )
        # Don't want to leave multiple files on disk, so delete
        # immediately after they're safely stored in the ZIP
        download_path.unlink()

    async def get_json(self, url: str, post: bool = False, **kwargs) -> dict[str, str]:
        """Get a JSON and return it as a dictionary."""
        response = await retry_async(
            self.session.post if post else self.session.get, args=[url], kwargs=kwargs
        )
        response_bytes = await retry_async(response.read)
        try:
            json_obj = json.loads(response_bytes.decode("utf-8"))
        except json.JSONDecodeError:
            raise AssertionError(f"Invalid JSON string: {response_bytes}")
        return json_obj

    async def get_hyperlinks(
        self,
        url: str,
        filter_pattern: typing.Pattern | None = None,
        verify: bool = True,
        headers: dict | None = None,
    ) -> dict[str, str]:
        """Return all hyperlinks from a specific web page.

        This is a helper function to perform very basic web-scraping functionality.
        It extracts all hyperlinks from a web page, and returns those that match
        a specified pattern. This means it can find all hyperlinks that look like
        a download link to a single data resource.

        Args:
            url: URL of web page.
            filter_pattern: If present, only return links that contain pattern.
            verify: Verify ssl certificate (EPACEMS https source has bad certificate).
            headers: Additional headers to send in the GET request.
        """
        # Parse web page to get all hyperlinks
        response = await retry_async(
            self.session.get,
            args=[url],
            kwargs={
                "ssl": verify,
                **({"headers": headers} if headers is not None else {}),
            },
        )
        text = await retry_async(response.text)
        return self.get_hyperlinks_from_text(text, filter_pattern, url)

    async def get_hyperlinks_via_playwright(
        self,
        url: str,
        playwright_browser: PlaywrightBrowser,
        filter_pattern: typing.Pattern | None = None,
    ) -> dict[str, str]:
        """Return all hyperlinks from a specific web page.

        This is a helper function to perform very basic web-scraping functionality.
        It extracts all hyperlinks from a web page, and returns those that match
        a specified pattern. This means it can find all hyperlinks that look like
        a download link to a single data resource.

        Args:
            url: URL of web page.
            playwright_browser: async browser instance to use for fetching the URL.
            filter_pattern: If present, only return links that contain pattern.
            verify: Verify ssl certificate (EPACEMS https source has bad certificate).
        """
        # Parse web page to get all hyperlinks
        page = await playwright_browser.new_page()
        await page.goto(url, timeout=10 * 60 * 1000)
        text = await page.content()
        return self.get_hyperlinks_from_text(text, filter_pattern, url)

    def get_hyperlinks_from_text(
        self,
        text: str,
        filter_pattern: typing.Pattern | None = None,
        context: str = "text",
    ) -> list[str]:
        """Return all hyperlinks from HTML text.

        This is a helper-helper function to perform very basic HTML-parsing functionality.
        It extracts all hyperlinks from an HTML text, and returns those that match
        a specified pattern. This means it can find all hyperlinks that look like
        a download link to a single data resource.

        Args:
            text: text containing HTML.
            filter_pattern: If present, only return links that contain pattern.
            context: String used in error messages to describe what text was being searched.
        """
        parser = _HyperlinkExtractor()
        parser.feed(text)

        # Filter to those that match filter_pattern
        hyperlinks = parser.hyperlinks

        if filter_pattern:
            hyperlinks = {
                link: name
                for link, name in hyperlinks.items()
                if filter_pattern.search(name) or filter_pattern.search(link)
            }

        # Warn if no links are found
        if not hyperlinks:
            self.logger.warning(
                f"In {context}: the archiver couldn't find any hyperlinks {('that match: ' + filter_pattern.pattern) if filter_pattern else ''}."
                f"Make sure your filter_pattern is correct, and check if the structure of the page is not what you expect it to be."
            )

        return hyperlinks

    def get_user_agent(self):
        """Get a random user agent from USER_AGENTS for use making requests.

        User agents represent the entity making the request. We typically make the
        request using the default aiohttp user-agent, but this can sometimes be
        blocked by the server. In this case, we use rotating user agents to successfully
        complete the request.
        """
        rand = randbelow(len(USER_AGENTS))
        return USER_AGENTS[rand]

    def _check_missing_files(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
    ) -> validate.DatasetUniversalValidation:
        """Check for any files from previous archive version missing in new version."""
        baseline_resources = set()
        if baseline_datapackage is not None:
            baseline_resources = {
                resource.name for resource in baseline_datapackage.resources
            }

        new_resources = {resource.name for resource in new_datapackage.resources}

        # Check for any files only in baseline_datapackage
        missing_files = baseline_resources - new_resources

        notes = []
        if len(missing_files) > 0:
            notes = [
                f"The following files would be deleted by new archive version: {missing_files}"
            ]

        return validate.DatasetUniversalValidation(
            name="Missing file test",
            description="Check for files from previous version of archive that would be deleted by the new version",
            success=len(missing_files) == 0,
            notes=notes,
            required_for_run_success=self.fail_on_missing_files,
        )

    def _check_file_size(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
    ) -> validate.DatasetUniversalValidation:
        """Check if any one file's size has changed by |>allowed_file_rel_diff|."""
        notes = []
        if baseline_datapackage is None:
            too_changed_files = False  # No files to compare to
        else:
            baseline_resources = {
                resource.name: resource for resource in baseline_datapackage.resources
            }
            too_changed_files = {}

            new_resources = {
                resource.name: resource for resource in new_datapackage.resources
            }

            # Check to see that file size hasn't changed by more than |>allowed_file_rel_diff|
            # for each dataset in the baseline datapackage
            for resource_name in baseline_resources:
                if any(
                    baseline_resources[resource_name].parts == parts
                    for parts in self.ignore_file_size_diff_partitions
                ):
                    continue
                if resource_name in new_resources:
                    try:
                        file_size_change = abs(
                            (
                                new_resources[resource_name].bytes_
                                - baseline_resources[resource_name].bytes_
                            )
                            / baseline_resources[resource_name].bytes_
                        )
                        if file_size_change > self.allowed_file_rel_diff:
                            too_changed_files.update({resource_name: file_size_change})
                    except ZeroDivisionError:
                        logger.warning(
                            f"Original file size was zero for {resource_name}. Ignoring file size check."
                        )

            if too_changed_files:  # If files are "too changed"
                notes = [
                    f"The following files have absolute changes in file size >|{self.allowed_file_rel_diff:.0%}|: {too_changed_files}"
                ]

        return validate.DatasetUniversalValidation(
            name="Individual file size test",
            description=f"Check for files from previous version of archive that have changed in size by more than {self.allowed_file_rel_diff:.0%}.",
            success=not bool(too_changed_files),  # If dictionary empty, test passes
            notes=notes,
            required_for_run_success=self.fail_on_file_size_change,
        )

    def _check_dataset_size(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
    ) -> validate.DatasetUniversalValidation:
        """Check if a dataset's overall size has changed by more than |>allowed_dataset_rel_diff|."""
        notes = []

        if baseline_datapackage is None:
            dataset_size_change = 0.0  # No change in size if no baseline
        else:
            baseline_size = sum(
                [resource.bytes_ for resource in baseline_datapackage.resources]
            )

            new_size = sum([resource.bytes_ for resource in new_datapackage.resources])

            # Check to see that overall dataset size hasn't changed by more than
            # |>allowed_dataset_rel_diff|
            dataset_size_change = abs((new_size - baseline_size) / baseline_size)
            if dataset_size_change > self.allowed_dataset_rel_diff:
                notes = [
                    f"The new dataset is {dataset_size_change:.0%} different in size than the last archive, which exceeds the set threshold of {self.allowed_dataset_rel_diff:.0%}."
                ]

        return validate.DatasetUniversalValidation(
            name="Dataset file size test",
            description=f"Check if overall archive size has changed by more than {self.allowed_dataset_rel_diff:.0%} from last archive.",
            success=dataset_size_change < self.allowed_dataset_rel_diff,
            notes=notes,
            required_for_run_success=self.fail_on_dataset_size_change,
        )

    def _check_data_continuity(
        self, new_datapackage: DataPackage
    ) -> validate.DatasetUniversalValidation:
        """Check that the archived data partitions are continuous and unique."""
        success = True
        note = []
        partition_to_test = []
        dataset_partitions = []

        # Unpack partitions of a dataset.
        for resource in new_datapackage.resources:
            if not resource.parts:  # Skip resources without partitions
                continue
            for partition_name, partition_values in resource.parts.items():
                if (
                    partition_name in VALID_PARTITION_RANGES
                ):  # If partition to be tested
                    partition_to_test += (
                        [partition_name]
                        if partition_name not in partition_to_test
                        else []
                    )  # Compile unique partition keys
                    dataset_partitions += (
                        partition_values
                        if isinstance(partition_values, list)
                        else [partition_values]
                    )  # Unpack lists where needed

        # Only perform this test if the part label is year quarter or year month
        # Note that this currently only works if there is one set of partitions,
        # and will fail if year_month and form are used to partition a dataset, e.g.
        if partition_to_test:
            if len(partition_to_test) == 1:
                interval = VALID_PARTITION_RANGES[partition_to_test[0]]
                expected_date_range = pd.date_range(
                    min(dataset_partitions), max(dataset_partitions), freq=interval
                )
                observed_date_range = pd.to_datetime(dataset_partitions)
                diff = expected_date_range.difference(observed_date_range)

                if observed_date_range.has_duplicates:
                    success = False
                    note = [
                        f"Partition contains duplicate time periods of data: f{observed_date_range.duplicated()}"
                    ]
                elif not diff.empty:
                    success = False
                    note = [
                        f"Downloaded partitions are not continuous. Missing the following {partition_to_test} partitions: {diff.to_numpy()}"
                    ]
                else:
                    success = True
                    note = ["All tested partitions are continuous and non-duplicated."]
            else:
                success = False
                note = [
                    f"The test is not configured to handle more than one partition tested at a time: {partition_to_test}"
                ]
        else:
            success = True
            note = [
                "The dataset partitions are not configured for this test, and the test was not run."
            ]

        return validate.DatasetUniversalValidation(
            name="Validate data continuity",
            description=f"Test {', '.join(list(VALID_PARTITION_RANGES.keys()))} partitions for continuity and duplication.",
            success=success,
            notes=note,
            required_for_run_success=self.fail_on_data_continuity,
        )

    def validate_dataset(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
    ) -> list[validate.ValidationTestResult]:
        """Run a series of validation tests for a new archive, and return results.

        Args:
            baseline_datapackage: DataPackage descriptor from previous version of archive.
            new_datapackage: DataPackage descriptor from newly generated archive.
            resources: Dictionary mapping resource name to ResourceInfo.

        Returns:
            Bool indicating whether or not all tests passed.
        """
        validations: list[validate.ValidationTestResult] = []

        # Run baseline set of validations for dataset using datapackage
        validations.append(
            self._check_missing_files(baseline_datapackage, new_datapackage)
        )
        validations.append(self._check_file_size(baseline_datapackage, new_datapackage))
        validations.append(
            self._check_dataset_size(baseline_datapackage, new_datapackage)
        )
        validations.append(self._check_data_continuity(new_datapackage))

        # Add per-file validations
        validations += self.file_validations

        # Add dataset-specific file validations
        validations += self.dataset_validate_archive(
            baseline_datapackage, new_datapackage, resources
        )

        return validations

    def dataset_validate_archive(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
    ) -> list[validate.DatasetUniversalValidation]:
        """Hook to add archive validation tests specific to each dataset."""
        return []

    def valid_year(self, year: int | str):
        """Check if this year is one we are interested in.

        Args:
            year: the year to check against our self.only_years
        """
        return (not self.only_years) or int(year) in self.only_years

    async def _unpack_resources(
        self,
    ) -> tuple[list[ArchiveAwaitable], list[Partitions]]:
        """Run ``get_resources`` method for archiver and separate partitons/resources.

        Some archivers will only return resources from ``get_resources``, while some
        will return tuples of resources and a dictionary of partitions that correspond
        to those resources. If no partitions are returned, then this function will
        return an empty list of partitions.
        """
        resources = []
        partitions = []
        async for resource in self.get_resources():
            if type(resource) is tuple:
                resource, partition = resource
                partitions.append(partition)
            resources.append(resource)

        return resources, partitions

    async def download_all_resources(
        self,
        retry_parts: list[Partitions] = [],
    ) -> typing.Generator[tuple[str, ResourceInfo]]:
        """Download all resources.

        This method uses the awaitables returned by `get_resources`. It
        coordinates downloading all resources concurrently.
        """
        # Get all awaitables from get_resources
        resources, partitions = await self._unpack_resources()

        if len(retry_parts) > 0:
            if len(partitions) == 0:
                raise RuntimeError(
                    "Archiver must return partions from `get_resources` to be able "
                    "to filter to a specific set of resources. See ferceqr archiver "
                    "for an example of how to implement this."
                )
            logger.info(f"Only downloading the following partitions: {retry_parts}")
            resources = [
                resource
                for resource, parts in zip(resources, partitions)
                if parts in retry_parts
            ]

        # Split resources into chunks to limit concurrency
        chunksize = self.concurrency_limit if self.concurrency_limit else len(resources)
        resource_chunks = [
            resources[i * chunksize : (i + 1) * chunksize]
            for i in range(math.ceil(len(resources) / chunksize))
        ]

        if self.concurrency_limit:
            self.logger.info("Downloading resources in chunks to limit concurrency")
            self.logger.info(f"Resource chunks: {len(resource_chunks)}")
            self.logger.info(f"Resources per chunk: {chunksize}")

        # Download resources concurrently and prepare metadata
        for resource_chunk in resource_chunks:
            for resource_coroutine in asyncio.as_completed(resource_chunk):
                # resource_coroutine can return list or individual resource
                # If individual resource, create list of 1 to make iterable
                if not isinstance(resources := await resource_coroutine, list):
                    resources = [resources]

                for resource_info in resources:
                    self.logger.info(f"Downloaded {resource_info.local_path}.")

                    # Perform various file validations
                    current_file_validations = [
                        validate.validate_filetype(
                            resource_info.local_path,
                            self.fail_on_empty_invalid_files,
                        ),
                        validate.validate_file_not_empty(
                            resource_info.local_path,
                            self.fail_on_empty_invalid_files,
                        ),
                        validate.validate_zip_layout(
                            resource_info.local_path,
                            resource_info.layout,
                            self.fail_on_empty_invalid_files,
                        ),
                    ]

                    # Check if there are failed file level validations
                    failed_validations = [
                        validation
                        for validation in current_file_validations
                        if not validation.success
                    ]
                    self.file_validations.extend(current_file_validations)
                    if len(failed_validations) > 0:
                        logger.error(
                            "The following validation tests failed with file-validation-fail-fast set:"
                            f" {[validation.name for validation in failed_validations]}"
                        )
                        self.failed_partitions[resource_info.local_path.name] = (
                            resource_info.partitions
                        )

                    # Return downloaded
                    yield str(resource_info.local_path.name), resource_info

            # If requested, create a new temporary directory per resource chunk
            if self.directory_per_resource_chunk:
                tmp_dir = tempfile.TemporaryDirectory()
                self.download_directory = Path(tmp_dir.name)
                self.logger.info(f"New download directory {self.download_directory}")

        # subclass cleanup when necessary
        await self.after_download()

    async def after_download(self) -> None:
        """Optional cleanup after download_all_resources for override by subclass as needed."""
        pass

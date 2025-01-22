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

import aiohttp
import pandas as pd

from pudl_archiver.archivers import validate
from pudl_archiver.frictionless import DataPackage, ResourceInfo
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

ArchiveAwaitable = typing.AsyncGenerator[
    typing.Awaitable[ResourceInfo | list[ResourceInfo]], None
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
        self.hyperlinks = set()
        super().__init__()

    def handle_starttag(self, tag, attrs):
        """Filter hyperlink tags and return href attribute."""
        if tag == "a":
            for attr, val in attrs:
                if attr == "href":
                    self.hyperlinks.add(val)


async def _download_file(
    session: aiohttp.ClientSession, url: str, file: Path | io.BytesIO, **kwargs
):
    async with session.get(url, **kwargs) as response:
        with file.open("wb") if isinstance(file, Path) else nullcontext(file) as f:
            async for chunk in response.content.iter_chunked(1024):
                f.write(chunk)


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

        # Create logger
        self.logger = logging.getLogger(f"catalystcoop.{__name__}")
        self.logger.info(f"Archiving {self.name}")

    @abstractmethod
    def get_resources(self) -> ArchiveAwaitable:
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
        raise RuntimeError(f"Failed to download valid zipfile from {url}")

    async def download_file(self, url: str, file_path: Path | io.BytesIO, **kwargs):
        """Download a file using async session manager.

        Args:
            url: URL to file to download.
            file_path: Local path to write file to disk or bytes object to save file in memory.
            kwargs: Key word args to pass to retry_async.
        """
        await retry_async(_download_file, [self.session, url, file_path], kwargs)

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

    async def get_json(self, url: str, **kwargs) -> dict[str, str]:
        """Get a JSON and return it as a dictionary."""
        response = await retry_async(self.session.get, args=[url], kwargs=kwargs)
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
            self.session.get,
            args=[url],
            kwargs={
                "ssl": verify,
                **({"headers": headers} if headers is not None else {}),
            },
        )
        text = await retry_async(response.text)
        parser.feed(text)

        # Filter to those that match filter_pattern
        hyperlinks = parser.hyperlinks
        if filter_pattern is not None:
            self.logger.info(f"Filtering using {filter_pattern}")
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

        notes = None
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
        notes = None
        if baseline_datapackage is None:
            too_changed_files = False  # No files to compare to
        else:
            baseline_resources = {
                resource.name: resource.bytes_
                for resource in baseline_datapackage.resources
            }
            too_changed_files = {}

            new_resources = {
                resource.name: resource.bytes_ for resource in new_datapackage.resources
            }

            # Check to see that file size hasn't changed by more than |>allowed_file_rel_diff|
            # for each dataset in the baseline datapackage
            for resource_name in baseline_resources:
                if resource_name in new_resources:
                    try:
                        file_size_change = abs(
                            (
                                new_resources[resource_name]
                                - baseline_resources[resource_name]
                            )
                            / baseline_resources[resource_name]
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
        notes = None

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
        note = None
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

    async def download_all_resources(
        self,
    ) -> typing.Generator[tuple[str, ResourceInfo], None, None]:
        """Download all resources.

        This method uses the awaitables returned by `get_resources`. It
        coordinates downloading all resources concurrently.
        """
        # Get all awaitables from get_resources
        resources = [resource async for resource in self.get_resources()]

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
                    self.file_validations.extend(
                        [
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
                    )

                    # Return downloaded
                    yield str(resource_info.local_path.name), resource_info

            # If requested, create a new temporary directory per resource chunk
            if self.directory_per_resource_chunk:
                tmp_dir = tempfile.TemporaryDirectory()
                self.download_directory = Path(tmp_dir.name)
                self.logger.info(f"New download directory {self.download_directory}")

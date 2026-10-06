"""Download FERC EQR data."""

import asyncio
import logging
import re
from pathlib import Path

import pandas as pd
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    Partitions,
    ResourceInfo,
)
from pudl_archiver.utils import retry_async

logger = logging.getLogger(f"catalystcoop.{__name__}")
YEAR_QUARTER_PATT = re.compile(r"CSV_(\d{4})_Q(\d).zip")
FILE_NAME_PATT = re.compile(r"ferceqr-(\d{4}q\d)\.zip")
FIRST_YEAR_QUARTER = "2013q3"


class FercEQRArchiver(AbstractDatasetArchiver):
    """FERC EQR archiver.

    EQR data is much too large to use with Zenodo, so this archiver
    is meant to be used with be used with the `fsspec` storage
    backend. To run the archiver with this backend, execute the
    following command:

    ```
    pudl_archiver archive fsspec ferceqr gs://archives.catalyst.coop/ferceqr
    ```

    Only the ``datapackage.json`` metadata can be archived on Zenodo. After the data
    archive has been created, the following command creates an unpublished Zenodo
    draft with a copy of the ``datapackage.json``, stamped with the version and DOI of
    that draft (the same stamped copy is saved with the data):

    ```
    pudl_archiver archive fsspec-metadata ferceqr gs://archives.catalyst.coop/ferceqr
    ```
    """

    name = "ferceqr"
    concurrency_limit = 1
    directory_per_resource_chunk = True
    max_wait_time = 36000

    async def get_resources(self) -> tuple[ArchiveAwaitable, Partitions]:
        """Download FERC EQR resources."""
        # Dynamically get links to all quarters of EQR data. Loading the page
        # sometimes fails, for example when the browser's navigation is cancelled,
        # and trying again is cheap compared to failing the whole run.
        urls = await retry_async(
            self.get_urls,
            retry_on=(PlaywrightError,),
            retry_count=4,
            retry_base_s=5,
        )

        logger.info(f"Found {len(urls)} quarters of available EQR data.")
        year_quarters = {url: self._year_quarter_from_url(url) for url in urls}
        if not year_quarters:
            raise RuntimeError("Found no quarters of EQR data on the website.")

        # Fail now, instead of after downloading everything, if quarters are missing
        self._check_no_quarters_have_vanished(set(year_quarters.values()))
        self._check_quarters_are_continuous(sorted(year_quarters.values()))

        most_recent_quarter = max(year_quarters.values())
        # Don't fail on large size diffs for most recent quarter
        # We expect to see significant changes as new data is released
        logger.info(f"Ignoring size diffs for quarter: {most_recent_quarter}")
        year, quarter = most_recent_quarter.split("q")
        self.ignore_file_size_increase_partitions = [
            {"year_quarter": most_recent_quarter},
            # TODO: Remove once an archive with ``year_quarter`` partitions has been
            # published. Until then the previous archive's resources are partitioned
            # by ``year`` and ``quarter``, and that's what the size check compares.
            {"year": int(year), "quarter": int(quarter)},
        ]

        for url, year_quarter in year_quarters.items():
            partitions = {"year_quarter": year_quarter}
            yield self.get_quarter_csv(url, partitions), partitions

    @staticmethod
    def _year_quarter_from_url(url: str) -> str:
        """Get the quarter of a download link, formatted like ``2013q3``."""
        link_match = YEAR_QUARTER_PATT.search(url)
        return f"{link_match.group(1)}q{link_match.group(2)}"

    def _check_no_quarters_have_vanished(self, year_quarters: set[str]) -> None:
        """Check that every quarter in the previous archive is still available.

        The quarters on the website should always be the same as, or more than, the
        ones that we archived before. Files only vanish when FERC is in the middle
        of updating them, or by mistake, so don't build a new archive without them.
        """
        if self.baseline_datapackage is None or not self.fail_on_missing_files:
            return
        previous = {
            match.group(1)
            for resource in self.baseline_datapackage.resources
            if (match := FILE_NAME_PATT.search(resource.name))
        }
        vanished = sorted(previous - year_quarters)
        if vanished:
            raise RuntimeError(
                f"Quarters of EQR data that were archived before have vanished from "
                f"the website: {vanished}. FERC may be in the middle of updating its "
                "files, so try again later."
            )

    @staticmethod
    def _check_quarters_are_continuous(year_quarters: list[str]) -> None:
        """Check that there are no quarters missing from the first to the last one.

        The data starts in 2013q3. Quarters before that are on a different page.
        """
        if year_quarters[0] != FIRST_YEAR_QUARTER:
            raise RuntimeError(
                f"Expected EQR data to start in {FIRST_YEAR_QUARTER}, but the first "
                f"quarter available is {year_quarters[0]}."
            )
        expected = pd.period_range(year_quarters[0], year_quarters[-1], freq="Q")
        missing = [
            str(quarter).lower()
            for quarter in expected
            if str(quarter).lower() not in set(year_quarters)
        ]
        if missing:
            raise RuntimeError(
                f"Missing quarters of EQR data on the website: {missing}. "
                "FERC may be in the middle of updating its files, so try again later."
            )

    async def get_urls(self) -> list[str]:
        """Use playwright to dynamically grab URLs from the EQR webpage.

        Only grabs URLs that match the expected pattern for the quarterly CSV files.
        """
        logger.info(
            "Launching browser with playwright to get EQR year-quarter download links"
        )
        async with async_playwright() as pw:
            browser = await pw.webkit.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto("https://eqrreportviewer.ferc.gov/")
            # Navigate to Downlaods tab, and wait for tab to finish loading
            await page.get_by_text("Downloads", exact=True).click()
            await asyncio.sleep(10)

            # Find all links matching expected pattern and return
            return [
                await locator.get_attribute("href")
                for locator in await page.get_by_text(YEAR_QUARTER_PATT).all()
            ]

    async def get_quarter_csv(
        self, url: str, partitions: Partitions
    ) -> tuple[Path, dict]:
        """Download a quarter of 2013-present data."""
        # Extract year-quarter from URL
        logger.info(f"Found EQR data for {partitions['year_quarter']}")

        # Record server metadata before downloading, so we know what we archived
        source_metadata = await self.get_source_metadata(url)

        # Download quarter
        download_path = (
            self.download_directory / f"ferceqr-{partitions['year_quarter']}.zip"
        )
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions=partitions,
            source_metadata=source_metadata,
        )

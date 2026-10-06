"""Download FERC EQR data."""

import asyncio
import logging
import re
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    Partitions,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")
YEAR_QUARTER_PATT = re.compile(r"CSV_(\d{4})_Q(\d).zip")
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
        # Dynamically get links to all quarters of EQR data
        urls = await self.get_urls()

        # Check how many quarters of data we found from EQR webpage
        # The number we expect will change as data is released
        # but as of writing there are 53, so this provides reasonable lower bound
        logger.info(f"Found {len(urls)} quarters of available EQR data.")
        if len(urls) < 48:
            raise RuntimeError(
                "Expected a minimum of 48 quarters of EQR data to be available."
                f" Found the following URLs: {urls}"
            )

        year_quarters = {url: self._year_quarter_from_url(url) for url in urls}
        # Fail now, instead of after downloading everything, if there's a gap
        self._check_quarters_are_continuous(sorted(year_quarters.values()))

        most_recent_quarter = max(year_quarters.values())
        # Don't fail on large size diffs for most recent quarter
        # We expect to see significant changes as new data is released
        logger.info(f"Ignoring size diffs for quarter: {most_recent_quarter}")
        self.ignore_file_size_increase_partitions = [
            {"year_quarter": most_recent_quarter}
        ]

        for url, year_quarter in year_quarters.items():
            partitions = {"year_quarter": year_quarter}
            yield self.get_quarter_csv(url, partitions), partitions

    @staticmethod
    def _year_quarter_from_url(url: str) -> str:
        """Get the quarter of a download link, formatted like ``2013q3``."""
        link_match = YEAR_QUARTER_PATT.search(url)
        return f"{link_match.group(1)}q{link_match.group(2)}"

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

        # Download quarter
        download_path = (
            self.download_directory / f"ferceqr-{partitions['year_quarter']}.zip"
        )
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions=partitions,
        )

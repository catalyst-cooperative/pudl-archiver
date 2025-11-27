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


class FercEQRArchiver(AbstractDatasetArchiver):
    """FERC EQR archiver.

    EQR data is much too large to use with Zenodo, so this archiver
    is meant to be used with be used with the `fsspec` storage
    backend. To run the archiver with this backend, execute the
    following command:

    ```
    pudl_archiver --datasets ferceqr --deposition-path gs://archives.catalyst.coop/ferceqr --depositor fsspec
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
        # but as of writing there are 48, so this provides reasonable lower bound
        logger.info(f"Found {len(urls)} quarters of available EQR data.")
        if len(urls) < 48:
            raise RuntimeError(
                "Expected a minimum of 48 quarters of EQR data to be available."
                f" Found the following URLs: {urls}"
            )

        most_recent_quarter = {"year": 1990, "quarter": 1}
        for url in urls:
            link_match = YEAR_QUARTER_PATT.search(url)
            partitions = {
                "year": int(link_match.group(1)),
                "quarter": int(link_match.group(2)),
            }

            # Find most recent available quarter of data
            new_date = pd.to_datetime(f"{partitions['year']}-Q{partitions['quarter']}")
            most_recent_date = pd.to_datetime(
                f"{most_recent_quarter['year']}-Q{most_recent_quarter['quarter']}"
            )

            if new_date > most_recent_date:
                most_recent_quarter = partitions
            yield self.get_quarter_csv(url, partitions), partitions
        # Don't fail on large size diffs for most recent quarter
        # We expect to see significant changes as new data is released
        logger.info(
            f"Ignoring size diffs for quarter: {most_recent_quarter['year']}q{most_recent_quarter['quarter']}"
        )
        self.ignore_file_size_increase_partitions = [most_recent_quarter]

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
        logger.info(f"Found EQR data for {partitions['year']}q{partitions['quarter']}")

        # Download quarter
        download_path = (
            self.download_directory
            / f"ferceqr-{partitions['year']}q{partitions['quarter']}.zip"
        )
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions=partitions,
        )

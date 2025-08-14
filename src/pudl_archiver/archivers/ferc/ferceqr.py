"""Download FERC EQR data."""

import asyncio
import logging
import re
from pathlib import Path

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")
YEAR_QUARTER_PATT = re.compile(r"CSV_(\d+)_Q(\d).zip")


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

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC EQR resources."""
        # Dynamically get links to all quarters of EQR data
        urls = await self.get_urls()
        for url in urls:
            yield self.get_quarter_csv(url)

    async def get_urls(self) -> list[str]:
        """Use playwright to dynamically grab URLs from the EQR webpage."""
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
            await asyncio.sleep(5)

            # Find all links matching expected pattern and return
            return [
                await locator.get_attribute("href")
                for locator in await page.get_by_text(YEAR_QUARTER_PATT).all()
            ]

    async def get_quarter_csv(self, url: str) -> tuple[Path, dict]:
        """Download a quarter of 2013-present data."""
        # Extract year-quarter from URL
        link_match = YEAR_QUARTER_PATT.search(url)
        year = int(link_match.group(1))
        quarter = int(link_match.group(2))
        logger.info(f"Found EQR data for {year}-Q{quarter}")

        download_path = self.download_directory / f"ferceqr-{year}-Q{quarter}.zip"

        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "quarter": quarter},
        )

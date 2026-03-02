"""Archive FERC Central Identifier (CID) data."""

from datetime import datetime
from pathlib import Path

from bs4 import Tag
from dateutil import parser as date_parser
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

SOURCE_URL = (
    "https://data.ferc.gov/company-registration/ferc-company-identifier-listing/"
)


class FercCIDArchiver(AbstractDatasetArchiver):
    """FERC CID archiver."""

    name = "ferccid"
    fail_on_missing_files = (
        False  # each run will create a new file and delete the old one.
    )

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC CID resources."""
        last_updated = await self.get_last_updated_date(page_url=SOURCE_URL)
        year = last_updated.year
        month = str(last_updated.month).zfill(2)  # Zero pad to ISO-8601 compliance
        day = str(last_updated.day).zfill(2)  # Zero pad to ISO-8601 compliance
        if self.valid_year(year):
            dataset_path = (
                self.download_directory / f"ferccid-data-table-{year}-{month}-{day}.csv"
            )
            data_dictionary_path = (
                self.download_directory
                / f"ferccid-data-dictionary-{year}-{month}-{day}.csv"
            )
            yield self.download_dataset(SOURCE_URL, download_path=dataset_path)
            yield self.download_data_dictionary(
                SOURCE_URL, download_path=data_dictionary_path
            )

    async def get_last_updated_date(self, page_url: str) -> datetime:
        """Get the Data Last Updated date from the FERC data viewer page."""
        soup = await self.get_soup(page_url)
        label_div = soup.find(
            lambda t: (
                isinstance(t, Tag)
                and t.name == "div"
                and t.get_text(strip=True) == "Data Last Updated"
            )
        )
        if not label_div:
            raise RuntimeError("Couldn't find 'Data Last Updated' label div")

        row = label_div.find_parent("div", class_="row")
        if not row:
            raise RuntimeError("Couldn't find parent row for 'Data Last Updated'")

        row_text = (
            row.get_text(" ", strip=True).replace("Data Last Updated", "").strip()
        )

        return date_parser.parse(row_text)

    async def download_dataset(
        self,
        page_url: str,
        download_path: Path,
        timeout_ms: int = 15_000,
    ) -> ResourceInfo:
        """Download FERC CID dataset using the Download button modal."""
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto(page_url, timeout=timeout_ms)

            # Click the main Download button
            # We specify this exactly to reduce possibility of selecting the wrong button
            await page.locator(
                "div.dataAsset div.fields.row div.d-flex.flex-row-reverse.mt-5 button.blueButton"
            ).click()

            # Wait for the pop-up download menu
            modal = page.locator("#downloadModal")
            await modal.wait_for(state="visible", timeout=timeout_ms)

            # Choose “Dataset"
            await modal.get_by_label("Dataset").click()

            # Choose CSV (this is the “File type” row)
            await modal.get_by_text("csv").click()

            # The modal has two buttons named “Download”; we want the one inside
            modal_download_button = modal.get_by_role("button", name="Download")

            # Fail fast if no download starts within the timeout
            try:
                async with page.expect_download(timeout=timeout_ms) as download_info:
                    await modal_download_button.click()
                download = await download_info.value
                await download.save_as(download_path)
            except PlaywrightTimeoutError as e:
                raise RuntimeError(
                    "Timed out waiting for FERC data‑dictionary CSV download"
                ) from e

            return ResourceInfo(
                local_path=download_path, partitions={"data_set": "data_table"}
            )

    async def download_data_dictionary(
        self,
        page_url: str,
        download_path: Path,
        timeout_ms: int = 15_000,
    ) -> ResourceInfo:
        """Download FERC CID data dictionary using the Download button modal."""
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto(page_url, timeout=timeout_ms)

            # Click the main Download button
            # We specify this exactly to reduce possibility of selecting the wrong button
            await page.locator(
                "div.dataAsset div.fields.row div.d-flex.flex-row-reverse.mt-5 button.blueButton"
            ).click()

            # Wait for the pop-up download menu
            modal = page.locator("#downloadModal")
            await modal.wait_for(state="visible", timeout=timeout_ms)

            # Choose “Data Dictionary”
            await modal.get_by_label("Data Dictionary").click()

            # Choose CSV (this is the “File type” row)
            await modal.get_by_text("csv").click()

            # The modal has two buttons named “Download”; we want the one inside
            modal_download_button = modal.get_by_role("button", name="Download")

            # Fail fast if no download starts within the timeout
            try:
                async with page.expect_download(timeout=timeout_ms) as download_info:
                    await modal_download_button.click()
                download = await download_info.value
                await download.save_as(download_path)
            except PlaywrightTimeoutError as e:
                raise RuntimeError(
                    "Timed out waiting for FERC data‑dictionary CSV download"
                ) from e

            return ResourceInfo(
                local_path=download_path, partitions={"data_set": "data_dictionary"}
            )

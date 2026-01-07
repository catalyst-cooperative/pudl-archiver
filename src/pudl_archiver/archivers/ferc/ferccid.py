"""Archive FERC Central Identifier (CID) data."""

from datetime import datetime
from pathlib import Path

from bs4 import Tag
from dateutil import parser as date_parser
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

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC CID resources."""
        last_updated = await self.get_last_updated_date(page_url=SOURCE_URL)
        year = last_updated.year
        month = last_updated.month
        day = last_updated.day
        if self.valid_year(year):
            dataset_path = (
                self.download_directory / f"ferccid-{year}-{month}-{day}.csv"
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
        timeout_ms: int = 60_000,
    ) -> ResourceInfo:
        """Download FERC CID dataset using the Download button modal."""
        async with async_playwright() as pw:
            browser = await pw.webkit.launch(headless=True)
            page = await browser.new_page()

            await page.goto(page_url, timeout=timeout_ms)

            download_button = page.get_by_role("button", name="Download")
            await download_button.click(timeout=timeout_ms)

            dataset_radio = page.locator('label:has-text("Dataset")')
            await dataset_radio.click()

            csv_option = page.locator("text=CSV")
            await csv_option.click(timeout=timeout_ms)

            modal_download_button = page.get_by_role("button", name="Download").last

            async with page.expect_download(timeout=timeout_ms) as download_info:
                await modal_download_button.click(timeout=timeout_ms)

            download = await download_info.value
            await download.save_as(download_path)

            return ResourceInfo(local_path=download_path, partitions={})

    async def download_data_dictionary(
        self,
        page_url: str,
        download_path: Path,
        timeout_ms: int = 60_000,
    ) -> ResourceInfo:
        """Download FERC CID data dictionary using the Download button modal."""
        async with async_playwright() as pw:
            browser = await pw.webkit.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(page_url, timeout=timeout_ms)

                download_button = page.get_by_role("button", name="Download")
                await download_button.click(timeout=timeout_ms)

                data_dictionary_radio = page.locator('label:has-text("Data Dictionary")')
                await data_dictionary_radio.click()

                csv_option = page.locator("text=CSV")
                await csv_option.click(timeout=timeout_ms)

                modal_download_button = page.get_by_role("button", name="Download").last

                async with page.expect_download(timeout=timeout_ms) as download_info:
                    await modal_download_button.click(timeout=timeout_ms)

                download = await download_info.value
                await download.save_as(download_path)

                return ResourceInfo(local_path=download_path, partitions={})
            finally:
                await page.close()
                await browser.close()

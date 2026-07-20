"""Utilities for working with the ferc online web app which serves DBF and historical EQR data."""

import logging
from pathlib import Path
from typing import Any, Literal

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import ResourceInfo

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def get_resources_for_form(
    ferc_form: Literal["1", "2", "6", "60", "EQR"],
    years: list[int],
    partitions_base: dict[str, Any],
    download_directory: Path,
) -> ResourceInfo:
    """Download a resource corresponding to a single year/form combo from ferc online app."""
    logger.info(f"Downloading the following years for ferc{ferc_form}: {years}")
    async with async_playwright() as pw:
        browser = await pw.webkit.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://forms.ferc.gov/")

        # Navigate to form specific page
        await page.locator(f"#lnkFormData{ferc_form}").click()

        # Loop through all years and download
        resources = []
        for year in years:
            logging.info(f"Attempting to download ferc{ferc_form} {year}")
            async with page.expect_download() as download_info:
                await page.locator(f"#Content1_lnk{year}f{ferc_form}").click()

            download = await download_info.value
            download_path = download_directory / f"ferc{ferc_form}-{year}.zip"
            await download.save_as(download_path)
            resources.append(
                ResourceInfo(
                    local_path=download_path,
                    partitions=partitions_base | {"year": year},
                )
            )
        return resources

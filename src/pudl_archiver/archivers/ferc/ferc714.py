"""Download FERC Form 714 data."""

from pathlib import Path

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import xbrl


class Ferc714Archiver(AbstractDatasetArchiver):
    """Ferc Form 714 archiver."""

    name = "ferc714"
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 714 resources."""
        yield self.get_bulk_csv()

        # Get XBRL filings
        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_714,
            self.download_directory,
            self.valid_year,
            self.session,
        )

    async def get_bulk_csv(self) -> tuple[Path, dict]:
        """Download historical CSVs of FERC Form 714 data from 2006-2010.

        Source page:
          https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric/data
        """
        url = "https://www.ferc.gov/sites/default/files/2021-06/Form-714-csv-files-June-2021.zip"
        download_path = self.download_directory / "ferc714.zip"
        async with async_playwright() as p:
            browser = await p.webkit.launch()
            self.download_zipfile_via_playwright(browser, url, download_path)
            await browser.close()
        return ResourceInfo(local_path=download_path, partitions={})

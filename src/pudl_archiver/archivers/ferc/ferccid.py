"""Archive FERC Central Identifier (CID) data."""

import re
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

SOURCE_CSV_URL = "https://ferc.gov/media/ferc-cid-listing-csv"
SOURCE_XLSX_URL = "https://www.ferc.gov/media/ferc-cid-listing"


BASE_URL = "https://ferc.gov/sites/default/files/"


class FercCIDArchiver(AbstractDatasetArchiver):
    """FERC CID archiver."""

    name = "ferccid"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC CID resources."""
        # get CSV
        # https://ferc.gov/sites/default/files/2025-06/FERC CID Listing 6-10-2025.csv
        csv_link_pattern = re.compile(
            r"FERC\sCID\sListing\s(\d{1,2})-(\d{1,2})-(\d{4}).csv"
        )
        links = await self.get_cid_hyperlinks(SOURCE_CSV_URL, csv_link_pattern)
        for href in links:
            # href: /sites/default/files/2025-06/FERC%20CID%20Listing%206-10-2025.csv
            filename = Path(
                urlparse(href).path
            ).name  # -> 'FERC%20CID%20Listing%206-10-2025.csv'
            decoded_filename = unquote(filename)  # FERC CID Listing 6-10-2025.csv
            matches = csv_link_pattern.search(decoded_filename)
            if not matches:
                continue
            month = int(matches.group(1))
            day = int(matches.group(2))
            year = int(matches.group(3))
            url = urljoin(BASE_URL, href)
            download_path = (
                self.download_directory / f"ferccid-{year}-{month}-{day}.csv"
            )
            if self.valid_year(year):
                yield self.get_cid_file(url, download_path)

        # get XLSX
        # https://www.ferc.gov/sites/default/files/2025-08/August_2025_CID.xlsx
        xlsx_link_pattern = re.compile(r"(\d{4})-(\d{2}).+_CID.xlsx")
        links = await self.get_cid_hyperlinks(SOURCE_XLSX_URL, xlsx_link_pattern)
        for href in links:
            # href: https://www.ferc.gov/sites/default/files/2025-08/August_2025_CID.xlsx
            path = urlparse(href).path
            matches = xlsx_link_pattern.search(path)
            if not matches:
                continue
            year = int(matches.group(1))
            month = int(matches.group(2))
            url = urljoin(BASE_URL, href)
            download_path = self.download_directory / f"ferccid-{year}-{month}.xlsx"
            if self.valid_year(year):
                yield self.get_cid_file(url, download_path)

    async def get_cid_hyperlinks(self, source_url, link_pattern):
        """Get hyperlinks that follow a link pattern from a source URL."""
        async with async_playwright() as p:
            browser = await p.webkit.launch()
            links = await self.get_hyperlinks_via_playwright(
                source_url, browser, link_pattern
            )
            await browser.close()
        return links

    async def get_cid_file(self, url, download_path) -> ResourceInfo:
        """Download FERC CID file."""
        async with async_playwright() as p:
            browser = await p.webkit.launch()
            await self.download_file_via_playwright(browser, url, download_path)
            await browser.close()

        return ResourceInfo(local_path=download_path, partitions={})

"""Download EPA PCAP data."""

import re
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = (
    "https://www.epa.gov/inflation-reduction-act/priority-climate-action-plan-directory"
)
DATA_TABLE_URLS = [
    "https://www.epa.gov/inflation-reduction-act/ghg-inventory-searchable-table",
    "https://www.epa.gov/inflation-reduction-act/ghg-reduction-measures-searchable-table",
    "https://www.epa.gov/inflation-reduction-act/co-pollutant-benefits-searchable-table",
    "https://www.epa.gov/inflation-reduction-act/lidac-benefits-searchable-table",
]


class EpaPcapArchiver(AbstractDatasetArchiver):
    """EPA PCAP archiver."""

    name = "epapcap"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA PCAP resources."""
        yield self.get_resource()

    async def get_resource(self) -> ResourceInfo:
        """Download EPA PCAP resources."""
        zip_path = self.download_directory / "epapcap.zip"
        data_paths_in_archive = set()
        # Download the three Excel files first
        excel_pattern = re.compile(r"priority.*\.xlsx")
        for link in await self.get_hyperlinks(BASE_URL, excel_pattern):
            await self.download_helper(
                Path(link).name, link, zip_path, data_paths_in_archive
            )

        # Download all PDFs from each searchable table
        to_fetch = {}
        pdf_pattern = re.compile(r".*\.pdf")
        for data_table_url in DATA_TABLE_URLS:
            for link in await self.get_hyperlinks(data_table_url, pdf_pattern):
                # Sometimes the links are absolute and sometimes relative, so we
                # get urljoin to convert them (it will leave the absolute links alone)
                link = urljoin(data_table_url, link)
                # separate protocol and query crud, if present
                link_split = urlsplit(link)
                filename = Path(link_split.path).name.lower()

                # hard-coded skip for this link, which is an inferior duplicate of
                # another link with the same filename
                if (
                    filename
                    == "maricopa-pinal-county-region-priority-climate-action-plan.pdf"
                    and link_split.netloc == "azmag.gov"
                ):
                    continue

                # Many of the PDFs are shared between the multiple searchable tables;
                # we only need one copy but let's make sure the URLs match
                if filename in to_fetch:
                    assert to_fetch[filename] == link, (
                        f"Found more than one distinct URL for {filename}:\n{to_fetch[filename]}\n{link}"
                    )
                to_fetch[filename] = link
            self.logger.info(
                f"Identified {len(to_fetch)} total files to fetch after scraping {data_table_url}"
            )

        for filename, link in sorted(to_fetch.items()):
            await self.download_helper(filename, link, zip_path, data_paths_in_archive)

        return ResourceInfo(
            local_path=zip_path,
            partitions={},
            laybout=ZipLayout(file_paths=data_paths_in_archive),
        )

    async def download_helper(self, filename, link, zip_path, data_paths_in_archive):
        """Download file and add to archive."""
        download_path = self.download_directory / filename
        user_agent = self.get_user_agent()
        await self.download_file(
            link, download_path, headers={"User-Agent": user_agent}
        )
        # a couple of the PDF links have challenges in front of them that
        # yield an HTML error page instead if the requestor doesn't act
        # like a web browser.
        # check that files that claim to be PDFs are actually PDFs;
        # if they're HTML files instead, try playwright;
        # if that doesn't work, call for help.
        if filename.endswith(".pdf"):
            with download_path.open("rb") as f:
                header = f.read(128).lower().strip()
            if not header.startswith(b"%pdf-"):
                if header.startswith(b"<!doctype html>"):
                    self.logger.info(
                        f"Got HTML instead of PDF at {link}; trying playwright"
                    )
                    async with async_playwright() as p:
                        browser = await p.webkit.launch()
                        await self.download_file_via_playwright(
                            browser, link, download_path
                        )
                        await browser.close()
                    with download_path.open("rb") as f:
                        header = f.read(128).lower().strip()
                # fail if first try wasn't a PDF and wasn't HTML either
                # also fail if second try wasn't a PDF
                assert header.startswith(b"%pdf-"), (
                    f"Expected a pdf from {filename} at {link} but got {header}"
                )

        self.add_to_archive(
            zip_path=zip_path,
            filename=filename,
            blob=download_path.open("rb"),
        )
        data_paths_in_archive.add(filename)
        download_path.unlink()

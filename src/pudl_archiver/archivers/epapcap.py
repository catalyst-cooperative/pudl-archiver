"""Download EPA PCAP data."""

import re
from pathlib import Path

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
            await self.download_helper(link, zip_path, data_paths_in_archive)

        # Download all PDFs from each searchable table
        pdf_pattern = re.compile(r".*\.pdf")
        for data_table_url in DATA_TABLE_URLS:
            for link in await self.get_hyperlinks(data_table_url, pdf_pattern):
                # The second and third searchable tables links are relative
                # to the TLD, so we convert them to absolute links
                prefix = "https://www.epa.gov"
                if not link.startswith("http"):
                    link = prefix + link
                    await self.download_helper(link, zip_path, data_paths_in_archive)

        return ResourceInfo(
            local_path=zip_path,
            partitions={},
            laybout=ZipLayout(file_paths=data_paths_in_archive),
        )

    async def download_helper(self, link, zip_path, data_paths_in_archive):
        """Download file and add to archive."""
        filename = Path(link).name
        # Do nothing if we're going to end up duplicating a file
        # Many of the PDFs are shared between the multiple searchable tables
        if filename in data_paths_in_archive:
            return
        download_path = self.download_directory / filename
        await self.download_file(link, download_path)
        self.add_to_archive(
            zip_path=zip_path,
            filename=filename,
            blob=download_path.open("rb"),
        )
        data_paths_in_archive.add(filename)
        download_path.unlink()

"""Download USGS USWTDB data."""

import re
from urllib.parse import urlparse

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.sciencebase.gov/catalog/item/5e99a01082ce172707f6fd2a"


class UsgsUswtdbArchiver(AbstractDatasetArchiver):
    """USGS USWTDB archiver.

    Data is published almost quarterly (with some extra publicatons), so monthly
    continuous data is not expected.
    """

    name = "usgsuswtdb"
    fail_on_data_continuity = False

    async def get_resources(self) -> ArchiveAwaitable:
        """Download USWTDB resources."""
        link_pattern = re.compile(r"uswtdb_v(\d+)_(\d+)(?:_(\d+))?_(\d{8})\.zip")
        self.logger.info(f"Searching {BASE_URL} for hyperlinks matching {link_pattern}")
        data_links = await self.get_hyperlinks(BASE_URL, link_pattern)
        for link, name in data_links.items():
            self.logger.debug(f"Found link: {link}, name: {name}")
            matches = link_pattern.search(name)
            if not matches:
                continue

            date = matches.group(4)
            year, month = date[:4], date[4:6]
            year_month = f"{year}-{month}"
            if self.valid_year(int(year)):
                yield self.get_year_month_resource(link, year_month)

    async def get_year_month_resource(self, link: str, year_month: str) -> ResourceInfo:
        """Download zip file."""
        # Append hyperlink to base URL to get URL of file
        parsed_url = urlparse(BASE_URL)
        url = f"{parsed_url.scheme}://{parsed_url.netloc}{link}"
        download_path = self.download_directory / f"usgsuswtdb-{year_month}.zip"
        self.logger.debug(f"Attempting to download {url} to {download_path}")
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year_month": year_month}
        )

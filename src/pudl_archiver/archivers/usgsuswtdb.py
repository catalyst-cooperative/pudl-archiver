"""Download USGS USWTDB data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.sciencebase.gov/catalog/item/5e99a01082ce172707f6fd2a"


class UsgsUswtdbArchiver(AbstractDatasetArchiver):
    """USGS USWTDB archiver."""

    name = "usgsuswtdb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download USWTDB resources."""
        link_pattern = re.compile(r"uswtdb_v(\d+)_(\d+)(?:_(\d+))?_(\d{8})\.zip")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue

            date = matches.group(4)
            year_month = f"{date[:4]}-{date[4:6]}"
            if self.valid_year(year_month[:4]):
                yield self.get_year_month_resource(link, year_month)

    async def get_year_month_resource(self, link: str, year_month: str) -> ResourceInfo:
        """Download zip file."""
        # Append hyperlink to base URL to get URL of file
        url = f"{BASE_URL}/{link}"
        download_path = self.download_directory / f"usgsuswtdb-{year_month}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year_month": year_month}
        )

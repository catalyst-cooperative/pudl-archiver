"""Download EIA-923 data."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.eia.gov/electricity/data/eia923"


class Eia923Archiver(AbstractDatasetArchiver):
    """EIA 923 archiver."""

    name = "eia923"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-923 resources."""
        link_pattern = re.compile(r"f((923)|(906920))_(\d{4})(er)*\.zip")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            year = int(link_pattern.search(link).group(4))
            if self.valid_year(year):
                yield self.get_year_resource(link, year)

    async def get_year_resource(self, link: str, year: int) -> tuple[Path, dict]:
        """Download zip file."""
        url = f"{BASE_URL}/{link}"
        download_path = self.download_directory / f"eia923-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

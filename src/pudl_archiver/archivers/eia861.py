"""Download EIA-861 data."""
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.eia.gov/electricity/data/eia861"


class Eia861Archiver(AbstractDatasetArchiver):
    """EIA 861 archiver."""

    name = "eia861"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-861 resources."""
        link_pattern = re.compile(r"f861(\d{2,4}).zip")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_year_resource(link, link_pattern.search(link))

    async def get_year_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file."""
        # Use archive link if year is not most recent year
        url = f"{BASE_URL}/{link}"
        year = int(match.group(1))
        # Older file names only have last two digits of year in name
        # Convert to 4-digit years
        if year < 100 and year >= 90:
            year += 1900
        elif year < 100 and year < 90:
            year += 2000
        download_path = self.download_directory / f"eia861-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

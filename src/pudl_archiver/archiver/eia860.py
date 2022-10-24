"""Download EIA-860 data."""
import re
import typing
from pathlib import Path

from pudl_archiver.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable

BASE_URL = "https://www.eia.gov/electricity/data/eia860"


class Eia860Archiver(AbstractDatasetArchiver):
    """EIA 860 archiver."""

    name = "eia860"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-860 resources."""
        link_pattern = re.compile(r"eia860(\d{4}).zip")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_year_resource(link, link_pattern.search(link))

    async def get_year_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file."""
        # Append hyperlink to base URL to get URL of file
        url = f"{BASE_URL}/{link}"
        year = match.group(1)
        download_path = self.download_directory / f"eia860-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}

"""Download EIA-923 data."""
import re
import typing
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable

BASE_URL = "https://www.eia.gov/electricity/data/eia923"


class Eia923Archiver(AbstractDatasetArchiver):
    name = "eia923"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-923 resources."""
        link_pattern = re.compile(r"f((923)|(906920))_(\d{4})\.zip")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_year_resource(link, link_pattern.search(link))

    async def get_year_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file."""
        url = f"{BASE_URL}/{link}"
        year = match.group(4)
        download_path = self.download_directory / f"eia923-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}

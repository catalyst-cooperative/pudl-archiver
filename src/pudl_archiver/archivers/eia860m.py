"""Download EIA-860M data."""
import calendar
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.eia.gov/electricity/data/eia860m"


class Eia860MArchiver(AbstractDatasetArchiver):
    """EIA-860M archiver."""

    name = "eia860m"
    month_map = {
        name.lower(): number for number, name in enumerate(calendar.month_name)
    }

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-860M resources."""
        link_pattern = re.compile(r"([a-z]+)_generator(\d{4}).xlsx")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_year_month_resource(link, link_pattern.search(link))

    async def get_year_month_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download xlsx file."""
        url = f"https://eia.gov/{link}"
        year = match.group(2)
        month = self.month_map[match.group(1)]
        download_path = self.download_directory / f"eia860m-{year}-{month:02}.xlsx"
        await self.download_file(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year_month": f"{year}-{month:02}"}
        )

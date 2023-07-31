"""Download EIA Thermal Cooling Water data."""
import logging
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")
BASE_URL = "https://www.eia.gov/electricity/data/water"


class EiaWaterArchiver(AbstractDatasetArchiver):
    """EIA Thermal Cooling Water archiver."""

    name = "eiawater"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA Thermal Cooling Water resources."""
        link_pattern = re.compile(r"[Cc]ooling\w+([Ss]ummary|[Dd]etail)_(\d{4})\.xlsx")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            match = link_pattern.search(link)
            table = match.group(1).lower()
            year = int(match.group(2))
            if self.valid_year(year):
                yield self.get_year_resource(link, table, year)

    async def get_year_resource(
        self, link: str, table: str, year: int
    ) -> tuple[Path, dict]:
        """Download zip file."""
        url = f"{BASE_URL}/{link}"
        download_path = self.download_directory / f"eiawater-{year}-{table}.xlsx"
        await self.download_file(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "table_type": table}
        )

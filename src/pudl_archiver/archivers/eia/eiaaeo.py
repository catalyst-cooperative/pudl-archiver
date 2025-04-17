"""Download EIA Annual Energy Outlook data."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.eia.gov/opendata/bulk"


class EiaAeoArchiver(AbstractDatasetArchiver):
    """EIA AEO archiver."""

    name = "eiaaeo"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA AEO resources."""
        for year in range(2014, 2026):
            if self.valid_year(year) and year != 2024:  # Skip missing year
                yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download zip file."""
        filename = f"AEO{year}.zip"
        url = f"{BASE_URL}/{filename}"
        download_path = self.download_directory / f"eiaaeo-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

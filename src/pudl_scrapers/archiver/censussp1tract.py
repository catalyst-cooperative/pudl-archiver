"""Download US Census DP1 GeoDatabase."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable


class CensusDp1TractArchiver(AbstractDatasetArchiver):
    name = "censusdp1tract"

    def get_resources(self) -> ArchiveAwaitable:
        """Download Census DP1 resources."""
        yield self.get_resource()

    async def get_resource(self) -> tuple[Path, dict]:
        """Download zip file."""
        url = "https://www2.census.gov/geo/tiger/TIGER2010DP1/Profile-County_Tract.zip"
        download_path = self.download_directory / "censusdp1tract-2010.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": 2010}

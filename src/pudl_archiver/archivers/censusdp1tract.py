"""Download US Census DP1 GeoDatabase."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class CensusDp1TractArchiver(AbstractDatasetArchiver):
    """Census DP1 GeoDatabase archiver."""

    name = "censusdp1tract"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download Census DP1 resources."""
        yield self.get_resource()

    async def get_resource(self) -> tuple[Path, dict]:
        """Download zip file."""
        url = "https://www2.census.gov/geo/tiger/TIGER2010DP1/Profile-County_Tract.zip"
        download_path = self.download_directory / "censusdp1tract-2010.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": 2010})

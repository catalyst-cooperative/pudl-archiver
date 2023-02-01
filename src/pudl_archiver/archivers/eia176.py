"""Download EIA 176 data."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class Eia176Archiver(AbstractDatasetArchiver):
    """EIA 176 archiver."""

    name = "eia176"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download bulk EIA 176 data."""
        yield self.get_bulk_resource()

    async def get_bulk_resource(self) -> tuple[Path, dict]:
        """Download zip file."""
        # Use archive link if year is not most recent year
        url = "https://www.eia.gov/naturalgas/ngqs/all_ng_data.zip"
        download_path = self.download_directory / "eia176.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={})

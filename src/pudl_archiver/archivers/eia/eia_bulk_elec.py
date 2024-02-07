"""Download EIA electricity data in bulk."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EiaBulkElecArchiver(AbstractDatasetArchiver):
    """EIA bulk electricity archiver."""

    name = "eia_bulk_elec"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        yield self.get_bulk_resource()

    async def get_bulk_resource(self) -> tuple[Path, dict]:
        """Download zip file."""
        # Use archive link if year is not most recent year
        url = "https://api.eia.gov/bulk/ELEC.zip"
        download_path = self.download_directory / "eia_bulk_elec.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={})

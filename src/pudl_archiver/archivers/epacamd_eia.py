"""Download EPA CAMD data."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EpaCamdEiaArchiver(AbstractDatasetArchiver):
    """EPA CAMD archiver."""

    name = "epacmd_eia"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CAMD to EIA crosswalk resources."""
        yield self.get_crosswalk_zip()

    async def get_crosswalk_zip(self) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github."""
        url = (
            "https://github.com/USEPA/camd-eia-crosswalk/archive/refs/heads/master.zip"
        )
        download_path = self.download_directory / "epacamd_eia.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={})

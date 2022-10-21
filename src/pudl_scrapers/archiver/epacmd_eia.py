"""Download EPA CAMD data."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable


class EpaCamdEiaArchiver(AbstractDatasetArchiver):
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

        return download_path, {}

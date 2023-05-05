"""Download EPA CAMD data."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EpaCamdEiaArchiver(AbstractDatasetArchiver):
    """EPA CAMD archiver."""

    name = "epacamd_eia"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CAMD to EIA crosswalk resources."""
        for year in [2018, 2021]:
            yield self.get_crosswalk_zip(year)

    async def get_crosswalk_zip(self, year: int) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github.

        For the version of the crosswalk using 2018 data, download the base EPA repo. For 2021 outputs
        use our fork. If we decide to archive more years we can add infrastructure to dynamically run
        the crosswalk and only archive the outputs, but for now this is the simplest way to archive
        the years in use.
        """
        crosswalk_urls = {
            2018: "https://github.com/USEPA/camd-eia-crosswalk/archive/refs/heads/master.zip",
            2021: "https://github.com/catalyst-cooperative/camd-eia-crosswalk-2021/archive/refs/heads/main.zip",
        }
        download_path = self.download_directory / f"epacamd_eia_{year}.zip"
        await self.download_zipfile(crosswalk_urls[year], download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

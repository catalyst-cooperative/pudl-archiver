"""Download EIA NEMS Github respository."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EiaNEMSArchiver(AbstractDatasetArchiver):
    """EIA NEMS archiver."""

    name = "eianems"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CAMD to EIA crosswalk resources."""
        crosswalk_urls = {
            2023: "https://github.com/EIAgov/NEMS/archive/refs/tags/Initial-GitHub-Release.zip",
        }

        for year, url in crosswalk_urls.items():
            yield self.get_year_resource(year, url)

    async def get_year_resource(self, year: int, url: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a model that produced data for a year of
        AEO data. For example, the initial Github release produced data for the 2023 AEO,
        and has a partition of {year = 2023}.
        """
        download_path = self.download_directory / f"eianems_{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

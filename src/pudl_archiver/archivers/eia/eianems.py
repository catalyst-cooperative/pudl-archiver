"""Download EIA NEMS Github respository."""

import logging
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class EiaNEMSArchiver(AbstractDatasetArchiver):
    """EIA NEMS archiver."""

    name = "eianems"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CAMD to EIA crosswalk resources."""
        # TODO: Once #538 merges consider grabbing these hyperlinks from the releases page
        # ("https://github.com/EIAgov/NEMS/releases") dynamically. They can't be grabbed by
        # get_hyperlinks() currently. They will still need to be manually mapped to the year
        # of AEO data that they correspond to, but this would let us check for new releases.
        nems_urls = {
            "https://github.com/EIAgov/NEMS/archive/refs/tags/Initial-GitHub-Release.zip": 2023,
        }

        for link in nems_urls:
            yield self.get_year_resource(year=nems_urls[link], link=link)

    async def get_year_resource(self, year: int, link: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a model that produced data for a year of
        AEO data. For example, the initial Github release produced data for the 2023 AEO,
        and has a partition of {year = 2023}.
        """
        download_path = self.download_directory / f"eianems-{year}.zip"
        await self.download_zipfile(link, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

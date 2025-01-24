"""Download EIA NEMS Github respository."""

import logging
import re
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
        crosswalk_urls = {
            "https://github.com/EIAgov/NEMS/archive/refs/tags/Initial-GitHub-Release.zip": 2023,
        }

        pattern = re.compile(r"\.zip$")  # Grab the zip, not the tar.gz of each release

        for link in await self.get_hyperlinks(
            "https://github.com/EIAgov/NEMS/releases", pattern
        ):
            if link in crosswalk_urls:
                yield self.get_year_resource(year=crosswalk_urls[link], link=link)
            else:
                raise AssertionError(
                    "There's a new NEMS release! Map it to a year of AEO data to successfully archive all NEMS releases."
                )

    async def get_year_resource(self, year: int, link: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a model that produced data for a year of
        AEO data. For example, the initial Github release produced data for the 2023 AEO,
        and has a partition of {year = 2023}.
        """
        download_path = self.download_directory / f"eianems-{year}.zip"
        await self.download_zipfile(link, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

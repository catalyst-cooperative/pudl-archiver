"""Download USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class UsgsUsPvDbArchiver(AbstractDatasetArchiver):
    """USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database.
    This dataset is mainly static with versions that are issued as separate datasets. As of
    Jan 2025, there are 2 Child items (versions) viewable at 
    https://www.sciencebase.gov/catalog/item/66707f69d34e89718fa3f82f (United States 
    Large-Scale Solar Photovoltaic Database).
    
    The most recent version is also available via a link called "CSV format" (Tabular Format)
    at https://energy.usgs.gov/uspvdb/data/" but the filename will include the date of the release
    which is not predictable. 

    This code will have to be updated if new versions are available. Maybe check for the number of 
    datasets returned in the catalog somehow?
    """

    name = "usgsuspvdb"
   
    async def get_resources(self) -> ArchiveAwaitable:
        """Download the 2 version of the database released in different years."""
        for year in [2023, 2024]:
            yield self.get_crosswalk_zip(year)

    async def get_crosswalk_zip(self, year: int) -> tuple[Path, dict]:
        """Download entire repo as a zipfile.

        .
        """
        crosswalk_urls = {
            2023: "https://www.sciencebase.gov/catalog/file/get/6442d8a2d34ee8d4ade8e6db",
            2024: "https://www.sciencebase.gov/catalog/file/get/6671c479d34e84915adb7536",
        }
        download_path = self.download_directory / f"usgsuspvdb_{year}.zip"
        await self.download_zipfile(crosswalk_urls[year], download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

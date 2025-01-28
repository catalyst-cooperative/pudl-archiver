"""Download USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.sciencebase.gov/catalog/items?parentId=66707f69d34e89718fa3f82f"


class UsgsUsPvDbArchiver(AbstractDatasetArchiver):
    """USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database.

    This dataset is mainly static with versions that are issued as separate datasets. As of
    Jan 2025, there are 2 Child items (versions) viewable at
    https://www.sciencebase.gov/catalog/item/66707f69d34e89718fa3f82f (United States
    Large-Scale Solar Photovoltaic Database).

    The most recent version is also available via a link called "CSV format" (Tabular Format)
    at https://energy.usgs.gov/uspvdb/data/" but the filename will include the date of the release
    which is not predictable.

    This code will have to be updated if new versions are available. It will raise an
    error if a version is found that isn't already mapped.
    """

    name = "usgsuspvdb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download the 2 version of the database released in different years."""
        # Get any link matching /item/alphanumeric and capture the alphanumeric part
        link_pattern = re.compile(r"\/item\/(\w+)$")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            dataset_id = link_pattern.search(link).group(1)
            yield self.get_version_resource(link, dataset_id)

    async def get_version_resource(
        self, link: str, dataset_id: str
    ) -> tuple[Path, dict]:
        """Download entire dataset as a zipfile for a given year.

        The `get` URLs are found on:
        * https://www.sciencebase.gov/catalog/item/6442d8a2d34ee8d4ade8e6db
        * https://www.sciencebase.gov/catalog/item/6671c479d34e84915adb7536

        The date on the file is the date of publication, and the time range covered
        by the dataset varies from version to version. To keep things simple, we map
        urls to their version manually and alert ourselves when any new data is published.
        """
        crosswalk_urls = {
            "6442d8a2d34ee8d4ade8e6db": "1.0",
            "6671c479d34e84915adb7536": "2.0",
        }
        try:
            version = crosswalk_urls[dataset_id]
        except KeyError:
            raise KeyError(
                f"Dataset ID {dataset_id} at link {link} isn't mapped to a version in crosswalk_urls. Is this a new version of data?"
            )
        download_path = self.download_directory / f"usgsuspvdb-{version}.zip"
        download_link = f"https://www.sciencebase.gov/catalog/file/get/{dataset_id}"

        await self.download_zipfile(download_link, download_path)

        return ResourceInfo(local_path=download_path, partitions={"version": version})

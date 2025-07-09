"""Download USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database."""

import re
from pathlib import Path
from urllib.parse import urljoin

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.sciencebase.gov"
PARENT_LINK = "/catalog/items?parentId=66707f69d34e89718fa3f82f"


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
        user_agent = self.get_user_agent()
        all_links = []

        # Get any link matching /item/alphanumeric and capture the alphanumeric part
        link_pattern = re.compile(r"\/item\/(\w+)$")
        for link in await self.get_hyperlinks(
            urljoin(BASE_URL, PARENT_LINK),
            link_pattern,
            headers={"User-Agent": user_agent},
        ):
            # One page captures the current version, while the other page has a
            # number of zipfiles representing each prior version published.
            # In both cases, we look for links matching the HTML tag
            # class="sb-file-get sb-download-link"
            soup = await self.get_soup(urljoin(BASE_URL, link))
            links = soup.findAll("span", {"class": "sb-download-link"})
            all_links.extend(links)

        # Deconstruct each returned match into a filename, file URL and version
        file_dict = [
            {
                "filename": url.text,
                "url": urljoin(BASE_URL, url.get("data-url")),
                "version": re.findall(r"v[0-9]_[0-9]", url.text),
            }
            for url in all_links
        ]

        # For each version, bundle together all the files corresponding to that version.
        for version in (url["version"][0] for url in file_dict if url["version"]):
            self.logger.info(f"Downloading files for version {version}.")
            version_file_dict = [url for url in file_dict if version in url["version"]]
            yield self.get_version_resource(version, version_file_dict)

        # Download any files that don't have a version
        for file in file_dict:
            if not file["version"]:
                self.logger.info(f"Downloading {file['filename']}.")
                yield self.get_other_resource(file)

    async def get_version_resource(
        self, version: str, version_file_dict: dict[str : str | list[str]]
    ) -> tuple[Path, dict]:
        """Download entire version of a dataset as a zipfile for a given year."""
        version = version.replace("_", "-")
        zip_path = self.download_directory / f"usgsuspvdb-{version}.zip"
        for record in version_file_dict:  # For each link in a version
            await self.download_add_to_archive_and_unlink(
                zip_path=zip_path,
                filename=record["filename"],
                url=urljoin(BASE_URL, record["url"]),
            )
        return ResourceInfo(local_path=zip_path, partitions={"version": version})

    async def get_other_resource(
        self, file_dict: dict[str : str | list[str]]
    ) -> tuple[Path, dict]:
        """Download resources without a version partition."""
        filename = file_dict["filename"]
        download_path = self.download_directory / filename
        await self.download_file(
            file_path=download_path,
            url=urljoin(BASE_URL, file_dict["url"]),
        )
        return ResourceInfo(local_path=download_path, partitions={})

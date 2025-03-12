"""Download NREL STS data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://data.nrel.gov/submissions/244"


class NrelStsArchiver(AbstractDatasetArchiver):
    """NREL STS archiver."""

    name = "nrelsts"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL STS resources."""
        # e.g.: https://data.openei.org/files/6219/DC-2022-LEAD-data.zip
        #       https://data.openei.org/files/6219/Data%20Dictionary%202022.xlsx
        #       https://data.openei.org/files/6219/LEAD%20Tool%20States%20List%202022.xlsx
        data_link_pattern = re.compile(r"files\/244\/([\w%-_])*(.zip|.xlsx|.pdf)")
        """Regex for matching the data files in a release on the OEDI page. Captures the year, and supports both .zip and .xlsx file names."""
        for data_link in await self.get_hyperlinks(BASE_URL, data_link_pattern):
            yield self.get_file_resource(data_link)

    async def get_file_resource(self, link: str) -> ResourceInfo:
        """Download all available data for a year.

        Resulting resource contains one zip file of CSVs per state/territory, plus a handful of .xlsx dictionary and geocoding files.

        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
        filename = link.split("/")[-1]
        filename = filename.replace("%20", "-").replace("_", "-").lower()
        download_path = self.download_directory / f"nrelsts-{filename}"
        await self.download_file(link, download_path)
        return ResourceInfo(
            local_path=download_path,
            partitions={"file_name": filename},
        )

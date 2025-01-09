"""Download US Census FIPS Codes."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www2.census.gov/programs-surveys/popest/geographies/"


class CensusFipsArchiver(AbstractDatasetArchiver):
    """Census FIPS Codes archiver."""

    name = "censusfips"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download Census FIPS Codes resources."""
        # the BASE_URL page has a bunch of links with YEAR/ at the end
        link_pattern = re.compile(r"(\d{4})/$")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download excel file."""
        # before 2017, the files are xls. after that its xlsx
        file_extension = "xlsx" if year >= 2017 else "xls"
        file_name = f"state-geocodes-v{year}.{file_extension}"
        url = f"{BASE_URL}/{year}/{file_name}"
        download_path = self.download_directory / file_name
        await self.download_file(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

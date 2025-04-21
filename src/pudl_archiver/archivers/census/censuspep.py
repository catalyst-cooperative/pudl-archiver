"""Download US Census Federal Information Processing Standards (FIPS) codes from Population Estimates Program (PEP)."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www2.census.gov/programs-surveys/popest/geographies"


class CensusPepArchiver(AbstractDatasetArchiver):
    """Census PEP FIPS Codes archiver."""

    name = "censuspep"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download Census PEP FIPS Codes resources."""
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
        """Download excel or txt file."""
        # every directory besides the 1990-2000 dir has excel files
        # that we have to sift through to find the correct file.
        # the oldest data (1990-2000) dir includes just one txt file.
        if year != 2000:
            link_url = f"{BASE_URL}/{year}"
            # before 2017, the files are xls. after that its xlsx
            link_pattern = re.compile(rf"all-geocodes-v{year}.(xlsx|xls)")
            file_names = await self.get_hyperlinks(link_url, link_pattern)
            if len(file_names) != 1:
                raise AssertionError(
                    f"We expected exactly one link for {year}, but we found: {file_names}"
                )
            file_name = list(file_names)[0]
        elif year == 2000:
            link_url = f"{BASE_URL}/1990-2000"
            file_name = "90s-fips.txt"
        url = f"{link_url}/{file_name}"
        download_path = self.download_directory / f"{self.name}-{year}.zip"
        await self.download_and_zip_file(url, file_name, download_path)
        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths={file_name}),
        )

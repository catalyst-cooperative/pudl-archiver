"""Archive EIA Manufacturing Energy Consumption Survey (MECS)."""

import logging
import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/consumption/manufacturing/data"
logger = logging.getLogger(f"catalystcoop.{__name__}")


class EiaMECSArchiver(AbstractDatasetArchiver):
    """EIA MECS archiver."""

    name = "eiamecs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-MECS resources."""
        years_url = "https://www.eia.gov/consumption/data.php#mfg"
        year_link_pattern = re.compile(r"(manufacturing/data/)(\d{4})/$")
        for link in await self.get_hyperlinks(years_url, year_link_pattern):
            match = year_link_pattern.search(link)
            year = match.groups()[1]
            yield self.get_year_resources(year)

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all excel tables for a year."""
        logger.info(f"Attempting to find resources for: {year}")
        data_paths_in_archive = set()
        year_url = f"{BASE_URL}/{year}"
        zip_path = self.download_directory / f"eiamecs-{year}.zip"
        if int(year) >= 2006:
            table_link_pattern = re.compile(
                r"(RSE|)[Tt]able(\d{1,2}|\d{1.1})_(\d{1,2})(.xlsx|.xls)"
            )
        elif int(year) == 2002:
            table_link_pattern = re.compile(
                r"(RSE|)[Tt]able(\d{1,2}).(\d{1,2})_\d{1,2}(.xlsx|.xls)"
            )
        elif int(year) == 1998:
            table_link_pattern = re.compile(
                r"(d|e)\d{2}([a-z]\d{1,2})_(\d{1,2})(.xlsx|.xls)"
            )
        elif int(year) == 1994:
            # These earlier years the pattern is functional but not actually that informative.
            # so we will just use the original name by making the whole pattern a match
            table_link_pattern = re.compile(
                r"((rse|)m\d{2}_(\d{2})([a-d]|)(.xlsx|.xls))"
            )
        elif int(year) == 1991:
            table_link_pattern = re.compile(r"((rse|)mecs(\d{2})([a-z]|)(.xlsx|.xls))")

        # Loop through all download links for tables
        for table_link in await self.get_hyperlinks(year_url, table_link_pattern):
            table_link = f"{year_url}/{table_link}"
            logger.info(f"Fetching {table_link}")
            # We are going to rename the files in a standard format by extracting
            # patterns from the table_link_pattern
            # From 1998 and before there are a bunch of letters in the file names
            # in patterns that are probably parsable somehow, but for now we are
            # just going to keep the original file names
            match = table_link_pattern.search(table_link)
            filename = match.group(1)
            if int(year) > 1998:
                is_rse = match.group(1)
                # there are several ways the they indicate that the files are
                # "data" vs "rse". we will add this to the end of the file name
                # but only for rse bc for many years data and the rse are together
                rse_map = {
                    "": "",
                    "d": "",
                    "RSE": "-rse",
                    "e": "-rse",
                }
                rse = rse_map[is_rse]
                major_num = match.group(2)
                minor_num = match.group(3)
                extension = match.group(4)
                # Download filename
                filename = (
                    f"eia-mecs-{year}-table-{major_num}-{minor_num}{rse}{extension}"
                )
            download_path = self.download_directory / filename
            await self.download_file(table_link, download_path)
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=download_path.open("rb"),
            )
            data_paths_in_archive.add(filename)
            # Don't want to leave multiple giant CSVs on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()

        resource_info = ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )
        return resource_info

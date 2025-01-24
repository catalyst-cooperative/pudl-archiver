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
        table_link_pattern = re.compile(r"[Tt]able(\d{1,2})_(\d{1,2}).xlsx")

        # Loop through all download links for tables
        data_paths_in_archive = set()
        year_url = f"{BASE_URL}/{year}"
        zip_path = self.download_directory / f"eiamecs-{year}.zip"
        for table_link in await self.get_hyperlinks(year_url, table_link_pattern):
            table_link = f"{year_url}/{table_link}"
            logger.info(f"Fetching {table_link}")
            # Get table major/minor number from links
            match = table_link_pattern.search(table_link)
            major_num, minor_num = match.group(1), match.group(2)

            # Download file
            filename = f"eia-mecs-{year}-table-{major_num}-{minor_num}.xlsx"
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
        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

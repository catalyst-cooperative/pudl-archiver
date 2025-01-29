"""Archive EIA Residential Energy Consumption Survey (RECS)."""

import logging
import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

LINK_PATTERNS = [
    # housing characteristics
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=characteristics",
        "prefix": "hc",
        "pattern": re.compile(r"HC (\d{1,2})\.(\d{1,2})\.xlsx"),
    },
    # consumption & expenditures
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=consumption",
        "prefix": "ce",
        "pattern": re.compile(r"ce(\d)\.(\d{1,2})([a-z]?)\.xlsx"),
    },
    # state data (housing characteristics)
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=state",
        "prefix": "state",
        "pattern": re.compile(r"State (.*)\.xlsx"),
    },
    # state data (consumption & expenditures)
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=state",
        "prefix": "state-ce",
        "pattern": re.compile(r"ce(\d)\.(\d{1,2})\.(.*)\.xlsx"),
    },
    # microdata
    # adding this in will require major changes+cleanup to the code below
    # {
    #    "base_url": "https://www.eia.gov/consumption/residential/data",
    #    "php_extension": "index.php?view=microdata",
    #    "prefix": "udata",
    #    "pattern": re.compile(r"(recs.*\d{4}.*public.*)\.(?:zip|csv|xlsx)", re.IGNORECASE),
    # }
]
logger = logging.getLogger(f"catalystcoop.{__name__}")


class EiaRECSArchiver(AbstractDatasetArchiver):
    """EIA RECS archiver."""

    name = "eiarecs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-RECS resources."""
        for year in [2020]:
            yield self.get_year_resources(year)

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all excel tables for a year."""
        # Loop through all download links for tables
        tables = []
        zip_path = self.download_directory / f"eia-recs-{year}.zip"
        data_paths_in_archive = set()
        # Loop through different categories of data (all .xlsx)
        for pattern_dict in LINK_PATTERNS:
            # Each category of data has its own url, etc.
            year_url = f"{pattern_dict['base_url']}/{year}"
            url = f"{year_url}/{pattern_dict['php_extension']}"
            table_link_pattern = pattern_dict["pattern"]
            for table_link in await self.get_hyperlinks(url, table_link_pattern):
                table_link = f"{year_url}/{table_link}"
                logger.info(f"Fetching {table_link}")
                # Get table major/minor number from links
                match = table_link_pattern.search(table_link)
                matched_metadata = (
                    "-".join(g for g in match.groups() if g).replace(" ", "_").lower()
                )
                output_filename = (
                    f"eia-recs-{year}-{pattern_dict['prefix']}-{matched_metadata}.xlsx"
                )

                # Download file
                download_path = self.download_directory / output_filename
                await self.download_file(table_link, download_path)
                self.add_to_archive(
                    zip_path=zip_path,
                    filename=output_filename,
                    blob=download_path.open("rb"),
                )
                data_paths_in_archive.add(output_filename)
                download_path.unlink()

        tables.append(
            ResourceInfo(
                local_path=zip_path,
                partitions={"year": year},
                layout=ZipLayout(file_paths=data_paths_in_archive),
            )
        )
        return tables

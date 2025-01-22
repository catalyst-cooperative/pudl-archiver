"""Archive EIA Residential Energy Consumption Survey (RECS)."""

# TODO:
# - grab all the data and then zip it up
# - make sure we're not missing anything with like ce1.2a.xlsx
# - do we want to zip everything up?
# - how to partition relative to the other tabs?
# - add in other years of data

import logging
import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

LINK_PATTERNS = [
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=characteristics",
        "prefix": "hc",
        "pattern": re.compile(r"HC (\d{1,2})\.(\d{1,2})\.xlsx"),
    },
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=consumption",
        "prefix": "ce",
        "pattern": re.compile(r"ce(\d)\.(\d{1,2})[a-z]?\.xlsx"),
    },
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=state",
        "prefix": "state",
        "pattern": re.compile(r"State (.*)\.xlsx"),
        "no_version": True,
    },
    {
        "base_url": "https://www.eia.gov/consumption/residential/data",
        "php_extension": "index.php?view=state",
        "prefix": "state-ce",
        "pattern": re.compile(r"ce(\d)\.(\d{1,2})\.(.*)\.xlsx"),
    },
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
        for pattern_dict in LINK_PATTERNS:
            year_url = f"{pattern_dict['base_url']}/{year}"
            url = f"{year_url}/{pattern_dict['php_extension']}"
            table_link_pattern = pattern_dict["pattern"]
            for table_link in await self.get_hyperlinks(url, table_link_pattern):
                table_link = f"{year_url}/{table_link}"
                logger.info(f"Fetching {table_link}")
                # Get table major/minor number from links
                match = table_link_pattern.search(table_link)
                output_filename = f"eia-recs-{year}-{pattern_dict['prefix']}"
                if "no_version" in pattern_dict and pattern_dict["no_version"]:
                    output_filename += "-" + match.group(1).lower().replace(" ", "-")
                else:
                    major_num, minor_num = (
                        match.group(1),
                        match.group(2),
                    )
                    output_filename += f"-{major_num}-{minor_num}"
                if len(match.groups()) >= 3:
                    output_filename += "-" + match.group(3)
                output_filename += ".xlsx"

                # Download file
                download_path = self.download_directory / output_filename
                await self.download_zipfile(table_link, download_path)

                tables.append(
                    ResourceInfo(
                        local_path=download_path,
                        partitions={"year": year},
                    )
                )
        return tables

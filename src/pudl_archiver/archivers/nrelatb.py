"""Download NREL ATB for Electricity Parquet data."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

# Note: Using non s3:// link here as compatibility between asyncio and botocore is
# complex.
BASE_URL = "https://oedi-data-lake.s3.amazonaws.com/ATB/electricity/parquet"
LINK_URL = "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2Felectricity%2Fparquet%2F"


class NrelAtbArchiver(AbstractDatasetArchiver):
    """NREL ATB for Electricity archiver."""

    name = "nrelatb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Using years gleaned from LINK_URL, iterate and download all files."""
        link_pattern = re.compile(r"parquet%2F(\d{4})")
        for link in await self.get_hyperlinks(LINK_URL, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download parquet file."""
        url = f"{BASE_URL}/{year}/ATBe.parquet"
        download_path = self.download_directory / f"nrelatb-{year}.parquet"
        await self.download_file(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

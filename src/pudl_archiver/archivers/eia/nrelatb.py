"""Download NREL ATB for Electricity Parquet data."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

# Note: Using non s3:// link here as compatibility between asyncio and botocore is
# complex.
BASE_URL = "https://oedi-data-lake.s3.amazonaws.com/ATB/electricity/parquet/"


class NrelAtbArchiver(AbstractDatasetArchiver):
    """NREL ATB for Electricity archiver."""

    name = "nrelatb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL ATB resources."""
        for year in range(2019, 2024):
            if self.valid_year(year):
                yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download parquet file."""
        url = f"{BASE_URL}/{year}/ATBe.parquet"
        download_path = self.download_directory / f"nrelatb-{year}.parquet"
        await self.download_file(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

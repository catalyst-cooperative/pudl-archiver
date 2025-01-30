"""Download NREL ATB for Electricity Parquet data."""

import re
from pathlib import Path
from typing import Literal
from urllib.parse import urljoin

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

# Note: Using non s3:// link here as compatibility between asyncio and botocore is
# complex.
BASE_URL = "https://oedi-data-lake.s3.amazonaws.com/ATB/electricity/parquet"
ELECTRICITY_LINK_URL = "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2Felectricity%2Fparquet%2F"
TRANSPORTATION_LINK_URL = "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2Ftransportation%2Fparquet%2F"


class NrelAtbArchiver(AbstractDatasetArchiver):
    """NREL ATB for Electricity archiver."""

    name = "nrelatb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Using years gleaned from LINK_URL, iterate and download all files."""
        link_pattern = re.compile(r"parquet%2F(\d{4})")
        for link in await self.get_hyperlinks(ELECTRICITY_LINK_URL, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_resource(year, "electricity")

    async def get_year_resource(
        self, year: int, atb_type: Literal["electricity"]
    ) -> tuple[Path, dict]:
        """Download parquet file."""
        zip_path = self.download_directory / f"nrelatb-{year}-{atb_type}.zip"
        data_paths_in_archive = set()
        # Get the parquet stuff
        parquet_url = f"{BASE_URL}/{year}/ATBe.parquet"
        parquet_filename = f"nrelatb-{year}-{atb_type}.parquet"
        await self.download_add_to_archive_and_unlink(
            parquet_url, parquet_filename, zip_path
        )
        data_paths_in_archive.add(parquet_filename)

        year_to_excel_url = {
            2024: f"https://atb.nrel.gov/{atb_type}/{year}/data",
            2023: f"https://atb.nrel.gov/{atb_type}/{year}/data",
            2022: "https://data.openei.org/submissions/5716",
            2021: "https://data.openei.org/submissions/4129",
            2020: f"https://atb-archive.nrel.gov/{atb_type}/{year}/data",
            2019: f"https://atb-archive.nrel.gov/{atb_type}/{year}/data",
        }
        year_excel_url = year_to_excel_url[year]
        link_pattern = re.compile(r"(.*)(.xlsx|.xlsm|.csv)$")

        for excel_url in await self.get_hyperlinks(year_excel_url, link_pattern):
            og_filename = (
                excel_url.split("/")[-1]
                .lower()
                .strip()
                .replace(" ", "-")
                .replace("_", "-")
                .replace(f"{year}-", "")
                .replace(f"{atb_type}-", "")
                .replace("atb-", "")
                .replace("%20", "-")
                .replace("annual-technology-baseline-", "")
            )
            excel_filename = f"nrelatb-{year}-{atb_type}-{og_filename}"
            excel_url = urljoin(year_excel_url, excel_url)
            await self.download_add_to_archive_and_unlink(
                excel_url, excel_filename, zip_path
            )
            data_paths_in_archive.add(excel_filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year, "data_type": atb_type},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

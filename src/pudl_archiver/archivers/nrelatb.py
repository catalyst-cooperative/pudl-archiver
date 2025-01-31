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
PARQUET_FILE_BASE_URL = "https://oedi-data-lake.s3.amazonaws.com/ATB/"
PARQUET_BASE_URL = (
    "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2F"
)


class NrelAtbArchiver(AbstractDatasetArchiver):
    """NREL ATB for Electricity archiver."""

    name = "nrelatb"

    async def get_resources(self) -> ArchiveAwaitable:
        """Using years gleaned from LINK_URL, iterate and download all files."""
        link_pattern = re.compile(r"parquet%2F(\d{4})")
        electricity_link = f"{PARQUET_BASE_URL}electricity%2Fparquet%2F"
        for link in await self.get_hyperlinks(electricity_link, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_electricity_resources(year, "electricity")

        link_pattern = re.compile(r"parquet%2F(\d{4})")
        transportation_link = f"{PARQUET_BASE_URL}transportation%2Fparquet%2F"
        for link in await self.get_hyperlinks(transportation_link, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_transportation_resources(year, "transportation")

    def clean_excel_filename(
        self, excel_url, year: int, atb_type: Literal["transportation", "electricity"]
    ) -> str:
        """Clean excel filename.

        We standardize the names to have this general structure:
        * nrelatb-{year}-{atb_type}-{cleaned up file name w/ extension}

        We attempt to convert all the spaces and other delimiters in the name
        to snakecase. We also remove the year and "ATB" or the full name in
        there.
        """
        og_filename = (
            excel_url.split("/")[-1]
            .lower()
            .strip()
            .replace(" ", "-")
            .replace("_", "-")
            .replace("(", "-")
            .replace(").", ".")
            .replace(f"{year}-", "")
            .replace(f"{atb_type}-", "")
            .replace("atb-", "")
            .replace("%20", "-")
            .replace("annual-technology-baseline-", "")
        )
        excel_filename = f"nrelatb-{year}-{atb_type}-{og_filename}"
        return excel_filename

    async def compile_parquet_urls(
        self, parquet_urls: list[str], url_to_check, year, atb_type
    ):
        """Recursively search within directories to find parquet files."""
        dir_pattern_str = r"%2F$"
        parquet_pattern_str = (
            rf"^{PARQUET_FILE_BASE_URL}{atb_type}/parquet/{year}/(.*).parquet$"
        )

        either_pattern = re.compile(
            rf"({parquet_pattern_str})|({dir_pattern_str})", re.IGNORECASE
        )
        for link in await self.get_hyperlinks(url_to_check, either_pattern):
            link = urljoin(url_to_check, link)
            if link.endswith("%2F"):
                # this is a directory... so we want to go deeper
                print(f"run this function recursively with: {link}")
                await self.compile_parquet_urls(parquet_urls, link, year, atb_type)
            elif link.endswith(".parquet"):
                print(f"add this link to parquet files: {link}")
                parquet_urls += [link]
        return parquet_urls

    async def get_year_electricity_resources(
        self, year: int, atb_type: Literal["electricity"]
    ) -> tuple[Path, dict]:
        """Download parquet file."""
        zip_path = self.download_directory / f"nrelatb-{year}-{atb_type}.zip"
        data_paths_in_archive = set()
        # Get the parquet stuff
        parquet_url = f"{PARQUET_FILE_BASE_URL}{atb_type}/parquet/{year}/ATBe.parquet"
        parquet_filename = f"nrelatb-{year}-{atb_type}.parquet"
        await self.download_add_to_archive_and_unlink(
            parquet_url, parquet_filename, zip_path
        )
        data_paths_in_archive.add(parquet_filename)
        # now get the excel/csv stuff
        # sometimes the files themselves are on these nrel.gov/type/year/data
        # pages but sometimes the data it on these data.openei.org pages which
        # are linked from the year/data pages.
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
            excel_filename = self.clean_excel_filename(excel_url, year, atb_type)
            excel_url = urljoin(year_excel_url, excel_url)
            await self.download_add_to_archive_and_unlink(
                excel_url, excel_filename, zip_path
            )
            data_paths_in_archive.add(excel_filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year, "sector": atb_type},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

    async def get_year_transportation_resources(
        self, year: int, atb_type: Literal["transportation"]
    ):
        """Get the transportation files."""
        zip_path = self.download_directory / f"nrelatb-{year}-{atb_type}.zip"
        data_paths_in_archive = set()

        async def _clean_and_download_parquet(parquet_file, parquet_dir):
            parquet_filename = (
                parquet_file.split("/")[-1]
                .lower()
                .strip()
                .replace(" ", "-")
                .replace("_", "-")
            )
            filename = f"nrelatb-{year}-{atb_type}-{parquet_filename}"
            url = urljoin(parquet_dir, parquet_file)
            await self.download_add_to_archive_and_unlink(url, filename, zip_path)

        year_parquet_url = f"{PARQUET_BASE_URL}{atb_type}%2Fparquet%2F{year}%2F"
        # TODO: convert this to a recursive function
        parquet_urls = []
        parquet_urls = await self.compile_parquet_urls(
            parquet_urls, year_parquet_url, year, atb_type
        )
        for parquet_url in parquet_urls:
            parquet_filename = (
                parquet_url.split("/")[-1]
                .lower()
                .strip()
                .replace(" ", "-")
                .replace("_", "-")
            )
            parquet_filename = f"nrelatb-{year}-{atb_type}-{parquet_filename}"
            await self.download_add_to_archive_and_unlink(
                parquet_url, parquet_filename, zip_path
            )
            data_paths_in_archive.add(parquet_filename)

        # now for the excel/csv files
        year_excel_url = f"https://atb.nrel.gov/{atb_type}/{year}/data"
        link_pattern = re.compile(r"(.*)(.xlsx|.xlsm|.csv)$")
        for excel_url in await self.get_hyperlinks(year_excel_url, link_pattern):
            excel_filename = self.clean_excel_filename(excel_url, year, atb_type)
            excel_url = urljoin(year_excel_url, excel_url)
            await self.download_add_to_archive_and_unlink(
                excel_url, excel_filename, zip_path
            )
            data_paths_in_archive.add(excel_filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year, "sector": atb_type},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

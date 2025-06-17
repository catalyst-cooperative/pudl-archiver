"""Download NREL ATB for Electricity Parquet data."""

import re
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
        atb_types = ["electricity", "transportation"]
        for atb_type in atb_types:
            base_url = f"{PARQUET_BASE_URL}{atb_type}%2Fparquet%2F"
            for link in await self.get_hyperlinks(base_url, link_pattern):
                matches = link_pattern.search(link)
                if not matches:
                    continue
                year = int(matches.group(1))
                # The default base url to grab the excel files works most of the time... but
                # many of the electricity years requires bespoke urls
                year_excel_base_url = f"https://atb.nrel.gov/{atb_type}/{year}/data"
                if (atb_type == "electricity") and (year <= 2022):
                    year_to_excel_url = {
                        2022: "https://data.openei.org/submissions/5716",
                        2021: "https://data.openei.org/submissions/4129",
                        2020: f"https://atb-archive.nrel.gov/{atb_type}/{year}/data",
                        2019: f"https://atb-archive.nrel.gov/{atb_type}/{year}/data",
                    }
                    year_excel_base_url = year_to_excel_url[year]
                if self.valid_year(year):
                    yield self.get_year_type_resources(
                        year, atb_type, year_excel_base_url
                    )

    def clean_excel_filename(
        self, excel_url, year: int, atb_type: Literal["transportation", "electricity"]
    ) -> str:
        """Clean excel filename.

        We standardize the names to have this general structure:
        * nrelatb-{year}-{atb_type}-{version if it exists}-{cleaned up file name w/ extension}

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
        self,
        parquet_urls: list[str],
        url_to_check: str,
        year: int,
        atb_type: Literal["transportation", "electricity"],
    ) -> list[str]:
        """Recursively search within directories to find parquet files.

        If
        """
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
                await self.compile_parquet_urls(parquet_urls, link, year, atb_type)
            elif link.endswith(".parquet"):
                # you found a pa add this link to parquet files
                parquet_urls += [link]
        return parquet_urls

    async def get_year_type_resources(
        self,
        year: int,
        atb_type: Literal["transportation", "electricity"],
        year_excel_base_url: str,
    ):
        """Get the transportation files."""
        zip_path = self.download_directory / f"nrelatb-{year}-{atb_type}.zip"
        data_paths_in_archive = set()

        year_parquet_url = f"{PARQUET_BASE_URL}{atb_type}%2Fparquet%2F{year}%2F"
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
            # Sometimes there's a version in the URL that
            # we also want to pull out.
            # If whatever precedes the filename in the URL looks like /v1.2.3./
            # We add it into the file name
            version_pattern = re.compile(r"v[\d.]+")
            version_match = re.match(version_pattern, parquet_url.split("/")[-2])
            if version_match:
                version = version_match.group().lower().strip().replace(".", "-")
                version = f"{version}-"
            else:
                version = ""
            parquet_filename = f"nrelatb-{year}-{atb_type}-{version}{parquet_filename}"
            await self.download_add_to_archive_and_unlink(
                parquet_url, parquet_filename, zip_path
            )
            data_paths_in_archive.add(parquet_filename)

        # now for the excel/csv files
        link_pattern = re.compile(r"(.*)(.xlsx|.xlsm|.csv|.zip)$")
        for excel_url in await self.get_hyperlinks(year_excel_base_url, link_pattern):
            excel_filename = self.clean_excel_filename(excel_url, year, atb_type)
            excel_url = urljoin(year_excel_base_url, excel_url)
            await self.download_add_to_archive_and_unlink(
                excel_url, excel_filename, zip_path
            )
            data_paths_in_archive.add(excel_filename)

        # The default base url to grab the excel files contains some of the
        # data (the link to the CSVs and the most recent versions)
        # For older versions of the data, we also want to grab files from the OpenEI
        # page linked on the page (if it exists, after 2022 and only for electricity).
        if year > 2022:
            oedi_link_pattern = re.compile(r"data.openei.org\/submissions\/(\d)")
            oedi_links = await self.get_hyperlinks(
                year_excel_base_url, oedi_link_pattern
            )
            for url in oedi_links:
                # If there's an OEDI page linked
                # On the OEDI page, iterate through and add any files we missed
                for oedi_url in await self.get_hyperlinks(url, link_pattern):
                    oedi_filename = self.clean_excel_filename(oedi_url, year, atb_type)
                    if oedi_filename not in data_paths_in_archive:
                        self.logger.info([url, oedi_url])
                        oedi_url = urljoin(url, oedi_url)
                        await self.download_add_to_archive_and_unlink(
                            oedi_url, oedi_filename, zip_path
                        )
                        data_paths_in_archive.add(oedi_filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year, "sector": atb_type},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

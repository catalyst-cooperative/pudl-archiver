"""Download NREL ATB for Electricity Parquet data."""

import re
from pathlib import Path
from typing import Literal
from urllib.parse import urljoin

from aiobotocore.session import get_session
from botocore import UNSIGNED
from botocore.config import Config

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

# Note: Using non s3:// link here as compatibility between asyncio and botocore is
# complex.
S3_VIEWER_FILE_BASE_URL = "https://oedi-data-lake.s3.amazonaws.com/ATB/"
S3_VIEWER_BASE_URL = (
    "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=ATB%2F"
)


class NrelAtbArchiver(AbstractDatasetArchiver):
    """NREL ATB for Electricity archiver."""

    name = "nrelatb"
    bucket = "oedi-data-lake"
    folder = "ATB"

    async def get_resources(self) -> ArchiveAwaitable:
        """Using years gleaned from LINK_URL, iterate and download all files."""
        session = get_session()

        # The links to these files no longer appear on the webpage, so we
        # use S3 to get the entire list of the bucket contents. To avoid
        # adding additional methods to handle botocore objects, we continue to
        # download these files through HTTPS.
        async with session.create_client(
            "s3", config=Config(signature_version=UNSIGNED)
        ) as client:
            paginator = client.get_paginator("list_objects_v2")
            all_files = []
            async for result in paginator.paginate(
                Bucket=self.bucket, Prefix=self.folder
            ):
                results = result.get("Contents", [])
                files = [
                    item["Key"] for item in results if not item["Key"].endswith("/")
                ]
                all_files.extend(files)

        # Since we no longer need this, we can close the client.
        await client.close()

        # For each ATB type, we get the list of years with data from these links
        # We'll create one file per atb_type per year.
        atb_types = ["electricity", "transportation"]
        for atb_type in atb_types:
            # First, get all relevant S3 links and use these
            # to ascertain the year range of this dataset
            s3_links = [file for file in all_files if atb_type in file]
            link_pattern = re.compile(r"(parquet|csv)\/(\d{4})")
            matches = [
                link_pattern.search(link)
                for link in s3_links
                if link_pattern.search(link)
            ]
            # Get the set of years for iteration
            years = {int(match.group(2)) for match in matches}

            # Then, download the files for each year
            for year in years:
                if self.valid_year(year):
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

                    year_s3_links = [link for link in s3_links if str(year) in link]

                    yield self.get_year_type_resources(
                        year, atb_type, year_excel_base_url, year_s3_links
                    )

    def clean_filename(
        self, url: str, year: int, atb_type: Literal["transportation", "electricity"]
    ) -> str:
        """Clean filename.

        We standardize the names to have this general structure:
        * nrelatb-{year}-{atb_type}-{version if it exists}-{cleaned up file name w/ extension}

        We attempt to convert all the spaces and other delimiters in the name
        to snakecase. We also remove the year and "ATB" or the full name in
        there.
        """
        og_filename = (
            url.split("/")[-1]
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
            .replace(".csv.csv", ".csv")  # Replace one duplicated filename
        )

        # Sometimes there's a version in the URL that
        # we also want to pull out.
        # If whatever precedes the filename in the URL looks like /v1.2.3./
        # We add it into the file name
        version_pattern = re.compile(r"v[\d.]+")
        version_match = re.match(version_pattern, url.split("/")[-2])
        if version_match:
            version = version_match.group().lower().strip().replace(".", "-")
            version = f"{version}-"
        else:
            version = ""

        # Put it all back together
        cleaned_filename = f"nrelatb-{year}-{atb_type}-{version}{og_filename}"
        return cleaned_filename

    async def download_s3_viewer_urls(
        self,
        s3_viewer_urls: list[str],
        data_paths_in_archive: set[str],
        zip_path: Path,
        year: int,
        atb_type: Literal["transportation", "electricity"],
        file_type: Literal["parquet", "csv"],
    ) -> set[str]:
        """Given a collection of parquet URLs, download add to archive.

        This function takes a collection of URLs compiled from the S3 bucket and
        constructs file names from the context, before downloading each file
        and adding it to the data paths in archive.
        """
        for s3_viewer_url in s3_viewer_urls:
            s3_viewer_filename = self.clean_filename(
                url=s3_viewer_url, year=year, atb_type=atb_type
            )
            await self.download_add_to_archive_and_unlink(
                s3_viewer_url, s3_viewer_filename, zip_path
            )
            data_paths_in_archive.add(s3_viewer_filename)
        return data_paths_in_archive

    async def get_year_type_resources(
        self,
        year: int,
        atb_type: Literal["transportation", "electricity"],
        year_excel_base_url: str,
        year_s3_links: list[str],
    ):
        """Get the files for a year and type (electricity/transport)."""
        zip_path = self.download_directory / f"nrelatb-{year}-{atb_type}.zip"
        data_paths_in_archive = set()

        # Compile URLs for Parquet and CSV files, download and add to archive
        for file_type in ["csv", "parquet"]:
            self.logger.info(
                f"{year}/{atb_type}: Downloading {file_type} data from the S3 viewer."
            )
            aws_base_url = "https://oedi-data-lake.s3.amazonaws.com/"
            # Combine the S3 file key from the bucket with the base URL to get a
            # download link
            s3_viewer_urls = [
                f"{aws_base_url}{link}" for link in year_s3_links if file_type in link
            ]

            data_paths_in_archive = await self.download_s3_viewer_urls(
                s3_viewer_urls=s3_viewer_urls,
                data_paths_in_archive=data_paths_in_archive,
                zip_path=zip_path,
                year=year,
                atb_type=atb_type,
                file_type=file_type,
            )

        # now for the excel/csv files
        link_pattern = re.compile(r"(.*)(.xlsx|.xlsm|.csv|.zip)$")
        self.logger.info(
            f"{year}/{atb_type}: Downloading spreadsheet data from {year_excel_base_url}."
        )
        for excel_url in await self.get_hyperlinks(year_excel_base_url, link_pattern):
            excel_filename = self.clean_filename(excel_url, year, atb_type)
            if excel_filename not in data_paths_in_archive:  # Avoid duplicates
                excel_url = urljoin(year_excel_base_url, excel_url)
                await self.download_add_to_archive_and_unlink(
                    excel_url, excel_filename, zip_path
                )
                data_paths_in_archive.add(excel_filename)

        # The default base url to grab the excel files contains some of the
        # data (the link to the CSVs and the most recent versions)
        # For newer versions of the data, we also want to grab files from the OpenEI
        # page linked on the page (if it exists, after 2022 and only for electricity).
        # This includes prior versions of Excel and Tableau worksheets.
        if year > 2022:
            self.logger.info(
                f"{year}/{atb_type}: Downloading additional data from OEDI."
            )
            oedi_link_pattern = re.compile(r"data.openei.org\/submissions\/(\d)")
            oedi_links = await self.get_hyperlinks(
                year_excel_base_url, oedi_link_pattern
            )
            for url in oedi_links:
                # If there's an OEDI page linked
                # On the OEDI page, iterate through and add any files we missed
                for oedi_url in await self.get_hyperlinks(url, link_pattern):
                    oedi_filename = self.clean_filename(oedi_url, year, atb_type)
                    if oedi_filename not in data_paths_in_archive:
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

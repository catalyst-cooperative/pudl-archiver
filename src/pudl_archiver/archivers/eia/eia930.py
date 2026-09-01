"""Download EIA-930 data."""

import re
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from playwright.async_api import async_playwright, expect

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/"
FILE_LIST_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_File_List_Meta.csv"
REFERENCE_URL = (
    "https://www.eia.gov/electricity/930-content/EIA930_Reference_Tables.xlsx"
)
ABOUT_URL = "https://www.eia.gov/electricity/gridmonitor/about"


class Eia930Archiver(AbstractDatasetArchiver):
    """EIA 930 archiver."""

    name = "eia930"

    async def get_eia930_file_list(self) -> pd.DataFrame:
        """Get EIA 930 file list dataframe."""
        return pd.read_csv(FILE_LIST_URL)

    async def get_reference_table(self) -> pd.DataFrame:
        """Get EIA 930 reference table."""
        ref_path = self.download_directory / "eia930-reference-tables.xlsx"
        await self.download_file(REFERENCE_URL, ref_path)
        return ResourceInfo(
            local_path=ref_path,
            partitions={"half_year": "all", "form": "reference"},
        )

    async def after_download(self) -> None:
        """Clean up playwright once downloads are complete."""
        await self.browser.close()
        await self.playwright.stop()

    async def get_eia930a_files(self) -> dict[str, str]:
        """Get a dictionary of EIA 930A file download URLs indexed by year."""
        link_dict = {}
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.webkit.launch()

        link_pattern = re.compile(r"EIA_930A_(\d{4})_with layout.xlsx")
        # Get main table links using playwright.
        page = await self.browser.new_page()
        await page.goto(ABOUT_URL, timeout=10 * 60 * 1000)
        await expect(
            page.get_by_text("About the EIA-930 data")
        ).to_be_visible()  # Wait for reference URL to load before proceeding.
        text = await page.content()
        links = self.get_hyperlinks_from_text(text, link_pattern, ABOUT_URL)

        for link in links:
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            link_dict.update({year: link})
        return link_dict

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-930 resources."""
        eia930_file_list = await self.get_eia930_file_list()
        year_period = (
            eia930_file_list[["YEAR", "PERIOD"]]
            .value_counts()
            .reset_index()
            .drop(columns=["count"])
            .sort_values("YEAR")
        )
        for index, period in year_period.iterrows():
            if self.valid_year(period.YEAR):
                yield self.get_eia930_half_year_resource(
                    file_list=eia930_file_list,
                    year=period.YEAR,
                    half_year=period.PERIOD,
                )

        eia930a_file_list = await self.get_eia930a_files()
        for year in eia930a_file_list:
            if self.valid_year(year):
                yield self.get_eia930a_year_resource(
                    file=eia930a_file_list[year], year=year
                )
        yield self.get_reference_table()

    async def get_eia930_half_year_resource(
        self, file_list: pd.DataFrame, year=int, half_year=int
    ) -> tuple[Path, dict]:
        """Download zip file of all files in a half-year."""
        self.logger.debug(f"Downloading EIA 930 data for {year}half{half_year}.")
        zip_path = self.download_directory / f"eia930-{year}half{half_year}.zip"
        data_paths_in_archive = set()
        period_files = file_list[
            (year == file_list.YEAR) & (half_year == file_list.PERIOD)
        ]
        for index, file in period_files.iterrows():
            url = BASE_URL + file.FILENAME
            filename = f"eia930-{year}half{half_year}-{file.DESCRIPTION.lower()}.csv"
            download_path = self.download_directory / filename
            await self.download_file(url, download_path)
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
            partitions={"half_year": f"{year}half{half_year}", "form": "eia930"},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

    async def get_eia930a_year_resource(
        self, file: str, year=int, half_year=int
    ) -> tuple[Path, dict]:
        """Download zip file of all files in year for EIA 930-A."""
        self.logger.debug(f"Downloading EIA930A data for {year}.")
        zip_path = self.download_directory / f"eia930a-{year}.zip"
        data_paths_in_archive = set()

        url = urljoin(ABOUT_URL, file)
        file_type = url.split(".")[
            -1
        ]  # Infer filetype based on url, rather than assigning
        filename = f"eia930a-{year}.{file_type}"
        await self.download_and_zip_file(url, filename, zip_path)
        data_paths_in_archive.add(filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year, "form": "eia930a"},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

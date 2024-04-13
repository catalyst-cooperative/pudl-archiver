"""Download EIA-930 data."""

import logging
from pathlib import Path

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
)
from pudl_archiver.frictionless import ResourceInfo, ZipLayout

BASE_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/"
FILE_LIST_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_File_List_Meta.csv"

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Eia930Archiver(AbstractDatasetArchiver):
    """EIA 930 archiver."""

    name = "eia930"

    async def get_file_list(self) -> pd.DataFrame:
        """Get EIA 930 file list dataframe."""
        return pd.read_csv(FILE_LIST_URL)

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-930 resources."""
        file_list = await self.get_file_list()
        year_period = (
            file_list[["YEAR", "PERIOD"]]
            .value_counts()
            .reset_index()
            .drop(columns=["count"])
            .sort_values("YEAR")
        )
        for _, period in year_period.iterrows():
            if self.valid_year(period.YEAR):
                yield self.get_year_resource(
                    file_list=file_list, year=period.YEAR, half=period.PERIOD
                )

    async def get_year_resource(
        self, file_list: pd.DataFrame, year=int, half=int
    ) -> tuple[Path, dict]:
        """Download zip file of all files in year."""
        logger.debug(f"Downloading data for {year}h{half}.")
        archive_path = self.download_directory / f"eia930-{year}h{half}.zip"
        data_paths_in_archive = set()
        period_files = file_list[(year == file_list.YEAR) & (half == file_list.PERIOD)]
        for _, file in period_files.iterrows():
            url = BASE_URL + file.FILENAME
            download_name = f"eia930-{year}h{half}-{file.DESCRIPTION.lower()}.csv"
            download_path = self.download_directory / download_name
            await self.download_file(url, download_path)
            self.add_to_archive(
                target_archive=archive_path,
                name=download_name,
                blob=download_path.open("rb"),
            )
            data_paths_in_archive.add(download_name)
            # Don't want to leave multiple giant CSVs on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()

        return ResourceInfo(
            local_path=archive_path,
            partitions={"year_half": f"{year}h{half}"},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

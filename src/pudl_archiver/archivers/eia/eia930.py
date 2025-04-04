"""Download EIA-930 data."""

from pathlib import Path

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/"
FILE_LIST_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_File_List_Meta.csv"


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
        for index, period in year_period.iterrows():
            if self.valid_year(period.YEAR):
                yield self.get_year_resource(
                    file_list=file_list, year=period.YEAR, half_year=period.PERIOD
                )

    async def get_year_resource(
        self, file_list: pd.DataFrame, year=int, half_year=int
    ) -> tuple[Path, dict]:
        """Download zip file of all files in year."""
        self.logger.debug(f"Downloading data for {year}half{half_year}.")
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
            partitions={"half_year": f"{year}half{half_year}"},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

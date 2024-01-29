"""Download EIA 191 data."""
import logging
import zipfile
from collections.abc import Iterable

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    EIANaturalGasData,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Eia191Archiver(AbstractDatasetArchiver):
    """EIA 191 archiver."""

    name = "eia191"
    base_url = "https://www.eia.gov/naturalgas/ngqs/data/report"

    async def get_datasets(self, url: str, form: str) -> Iterable[EIANaturalGasData]:
        """Return metadata for all forms for selected dataset."""
        datasets = await self.get_json(self.base_url)
        datasets = [EIANaturalGasData(**record) for record in datasets]
        datasets = [record for record in datasets if form in record.description]
        return datasets

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 191 resources."""
        datasets_list = await self.get_datasets(url=self.base_url, form="191")

        for dataset in datasets_list:
            if "Monthly" in dataset.description:  # Archive monthly, not annual data
                # Get all available years
                dataset_years = [year.ayear for year in dataset.available_years]
                for year in dataset_years:
                    yield self.get_year_resource(year, dataset)
            else:
                logger.info(f"Skipping archiving {dataset.description}")

    async def get_year_resource(
        self, year: str, dataset: EIANaturalGasData
    ) -> ResourceInfo:
        """Download all available data for a year.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.

        Args:
            year: the year we're downloading data for
        """
        archive_path = self.download_directory / f"eia191-{year}.zip"
        csv_name = f"eia191_{year}.csv"

        download_url = self.base_url + f"/{dataset.code}/data/{year}/{year}/ICA/Name"

        logger.info(f"Retrieving data for {year}")
        json_response = await self.get_json(download_url)
        dataframe = pd.DataFrame.from_dict(json_response["data"], orient="columns")

        # Rename columns
        column_dict = {
            item["field"]: str(item["headerName"])
            .lower()
            .replace("<br>", "_")
            .replace(" ", "_")
            for item in json_response["columns"]
        }
        dataframe = dataframe.rename(columns=column_dict).sort_values(
            ["company_name", "field_name"]
        )

        # Convert to CSV in-memory and write to .zip with stable hash
        csv_data = dataframe.to_csv(
            encoding="utf-8",
            index=False,
        )
        with zipfile.ZipFile(
            archive_path,
            "a",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            add_to_archive_stable_hash(
                archive=archive, filename=csv_name, data=csv_data
            )

        return ResourceInfo(
            local_path=archive_path,
            partitions={
                "year": year,
            },
            layout=ZipLayout(file_paths={csv_name}),
        )

"""Shared methods for data from EIA Natural Gas Quarterly Viewer (NGQV)."""

import logging
import zipfile
from collections.abc import Iterable

import pandas as pd
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash

logger = logging.getLogger(f"catalystcoop.{__name__}")


class EIANaturalGasData(BaseModel):
    """Data transfer object from EIA NGQV."""

    class Years(BaseModel):
        """Metadata about years for a specific dataset."""

        ayear: int

        class Config:  # noqa: D106
            alias_generator = to_camel
            populate_by_name = True

    code: str
    defaultsortby: str
    defaultunittype: str
    description: str
    last_updated: str
    available_years: list[Years]
    min_year: Years
    max_year: Years
    default_start_year: int
    default_end_year: int

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class EiaNGQVArchiver(AbstractDatasetArchiver):
    """EIA NGQV generic archiver. Subclass by form."""

    name: str
    form: str  # What to use to search for dataset in NGQV responses
    base_url: str = "https://www.eia.gov/naturalgas/ngqs/data/report"

    async def get_datasets(self, url: str, form: str) -> Iterable[EIANaturalGasData]:
        """Return metadata for all forms for selected dataset."""
        datasets = await self.get_json(self.base_url)
        datasets = [EIANaturalGasData(**record) for record in datasets]
        datasets = [record for record in datasets if form in record.description]
        return datasets

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA NGQV resources for specified form."""
        datasets_list = await self.get_datasets(url=self.base_url, form=self.form)

        for dataset in datasets_list:
            # Get all available years
            dataset_years = [year.ayear for year in dataset.available_years]
            for year in dataset_years:
                yield self.get_year_resource(year, dataset)

    async def get_year_resource(
        self, year: str, dataset: EIANaturalGasData
    ) -> ResourceInfo:
        """Download all available data for a year.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.

        Args:
            year: the year we're downloading data for
        """
        archive_path = self.download_directory / f"{self.name}-{year}.zip"
        csv_name = f"{self.name}_{year}.csv"

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
        dataframe = dataframe.rename(columns=column_dict)

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

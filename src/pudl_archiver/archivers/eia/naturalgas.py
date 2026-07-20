"""Shared methods for data from EIA Natural Gas Quarterly Viewer (NGQV).

The Natural Gas Quarterly Viewer allows us to export data from EIA Forms 176, 191 and
757A as individual "reports". A dataset's content may be split into multiple reports,
as it is for EIA 176, or be contained entirely in one report (as it is for EIA 757A).

"""

import zipfile
from collections.abc import Iterable

import pandas as pd
from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash


class EIANaturalGasData(BaseModel):
    """Data transfer object from EIA NGQV."""

    class Years(BaseModel):
        """Metadata about years for a specific dataset."""

        ayear: int

        class Config:  # noqa: D106
            alias_generator = to_camel
            populate_by_name = True

    report_code: str = Field(
        alias="code"
    )  # The shortname of the report (e.g., RP1, RPC)
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

    async def get_reports(self, url: str, form: str) -> Iterable[EIANaturalGasData]:
        """Return metadata for all forms for selected report."""
        reports = await self.get_json(self.base_url)
        reports = [EIANaturalGasData(**record) for record in reports]
        reports = [record for record in reports if form in record.description]
        return reports

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA NGQV resources for specified form."""
        reports_list = await self.get_reports(url=self.base_url, form=self.form)

        for report in reports_list:
            # Get all available years
            report_years = [year.ayear for year in report.available_years]
            for year in report_years:
                yield self.get_year_resource(year, report)

    async def get_year_partitions(self, year: str) -> dict[str, str]:
        """Define partitions for year resource. Override to handle complex partitions."""
        return {"year": year}

    def clean_header_name(self, col: pd.Series) -> pd.Series:
        """Perform a standard series of transformations on NGQV headername items."""
        return col.lower().replace("<br>", "_").replace(" ", "_")

    async def get_year_resource(
        self, year: str, report: EIANaturalGasData
    ) -> ResourceInfo:
        """Download all available data from one report for a year.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.

        Args:
            year: the year we're downloading data for
            report: the report we're downloading
        """
        archive_path = self.download_directory / f"{self.name}-{year}.zip"
        csv_name = f"{self.name}_{year}.csv"

        download_url = (
            self.base_url + f"/{report.report_code}/data/{year}/{year}/ICA/Name"
        )

        self.logger.info(f"Retrieving data for {year}")
        json_response = await self.get_json(download_url)
        dataframe = pd.DataFrame.from_dict(json_response["data"], orient="columns")

        # Rename columns
        column_dict = {
            item["field"]: self.clean_header_name(str(item["headerName"]))
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

        partitions = await self.get_year_partitions(year)

        return ResourceInfo(
            local_path=archive_path,
            partitions=partitions,
            layout=ZipLayout(file_paths={csv_name}),
        )

"""Download EIA 191 data."""

import logging
import zipfile
from collections import defaultdict

import pandas as pd

from pudl_archiver.archivers.classes import (
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.eia.naturalgas import EIANaturalGasData, EiaNGQVArchiver
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Eia191Archiver(EiaNGQVArchiver):
    """EIA 191 archiver."""

    name = "eia191"
    form = "191"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 191 resources, keeping monthly data only."""
        reports_list = await self.get_reports(url=self.base_url, form=self.form)
        # Annual and monthly data have different date ranges, but we want to end up
        # with one zipfile per year with one file per data format.
        # To do this, we iterate through the reports to get the total date range
        # and frequencies available for each report.
        freq_dict = defaultdict(set)
        for report in reports_list:
            for year in report.available_years:
                freq_dict[year.ayear].add(report.report_code)
        for year in freq_dict:
            reports = [
                report
                for report in reports_list
                if report.report_code in freq_dict[year]
            ]
            yield self.get_year_resource(year, reports)

    async def get_year_resource(
        self, year: str, reports: list[EIANaturalGasData]
    ) -> ResourceInfo:
        """Download all available data from one report for a year.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.

        Args:
            year: the year we're downloading data for
            report: the report we're downloading
        """
        archive_path = self.download_directory / f"{self.name}-{year}.zip"

        for report in reports:
            freq = (
                "monthly"
                if report.report_code == "RP8"
                else "annual"
                if report.report_code == "RP7"
                else ValueError(
                    f"Unexpected report code {report.report_code} for EIA 191 data"
                )
            )
            csv_name = f"{self.name}_{year}_{freq}.csv"

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

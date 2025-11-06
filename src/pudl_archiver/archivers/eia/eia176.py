"""Download EIA 176 data.

EIA 176 data is contained in three formats:
1. A bulk zipfile download (which also contains some EIA 191 data). As of 11/2025, this
    should contain all data but has some missing data relative to the reports that is
    not well explained.
2. Pre-defined reports which contain different slices of the EIA 176 survey data.
3. A custom report, which allows you to download a larger range of variables than are
    contained in the pre-defined reports. This includes all numeric data, but doesn't
    include absolutely everything found in the bulkfile (non-numeric variables aren't
    included.)

Unlike the other NGQV datasets (EIA 191 and 757A), the "reports" provided for EIA 176 do
not contain all fields of data. To get around this, we query the list of variable codes
for 176 from the portal, and then manually download subsets of data from all variable
codes for all years.

By archiving all three formats, we make sure we're capturing subtle variations in
data content across each format.
"""

import asyncio
import zipfile

import pandas as pd

from pudl_archiver.archivers.classes import (
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.eia.naturalgas import EIANaturalGasData, EiaNGQVArchiver
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash


class Eia176Archiver(EiaNGQVArchiver):
    """EIA 176 archiver."""

    name = "eia176"
    form = "176"
    data_url = "https://www.eia.gov/naturalgas/ngqs/data/report/RPC/data/"
    variables_url = "https://www.eia.gov/naturalgas/ngqs/data/items"
    bulk_url = "https://www.eia.gov/naturalgas/ngqs/all_ng_data.zip"

    async def get_variables(self, url: str = variables_url) -> list[str]:
        """Get list of variable codes from EIA NQGV portal.

        Each variable code corresponds to a variable that we can download from the custom
        report for EIA 176. We get all the variable codes
        """
        self.logger.info("Getting list of all variables in Form 176.")
        variables_response = await self.get_json(url)
        variables_list = [variable["item"] for variable in variables_response]
        return variables_list

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 176 resources, including all fields from the custom report."""
        # First grab the bulk data
        yield self.get_bulk_resource()

        # Then archive the already-defined reports
        reports_list = await self.get_reports(url=self.base_url, form=self.form)

        # Check that we don't have any unexpected reports here
        report_codes = {report.report_code for report in reports_list}
        expected_report_codes = {"RP1", "RP2", "RP3", "RP4", "RP5", "RP6", "RPC"}
        assert report_codes == expected_report_codes, (
            f"Got unexpected reports for 176: {report_codes - expected_report_codes}."
        )

        # For the custom fields, we want to ensure we're querying all possible
        # years of data availability across all EIA forms.
        # We do this by updating the all_report_years to include all possible years
        # covered by all other reports.
        all_report_years = set()

        for report in reports_list:
            # Get all available years
            if report.report_code != "RPC":  # We handle the custom report separately
                report_years = [year.ayear for year in report.available_years]
                all_report_years.update(report_years)  # Update the global list of years

        # Now, grab the data from the custom report
        variables_list = await self.get_variables(url=self.variables_url)

        for year in all_report_years:
            yield self.get_year_reports_and_custom_resource(
                str(year), reports_list, variables_list
            )

    async def get_bulk_resource(self) -> ResourceInfo:
        """Download the zipfile containing bulk zipped resources for EIA 176 and EIA 191."""
        download_path = self.download_directory / "eia176-bulk.zip"
        await self.download_zipfile(self.bulk_url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": "all", "format": "bulk"}
        )

    async def get_year_partitions(self, year: str) -> dict[str, str]:
        """Define partitions for year resource. Override to handle complex partitions."""
        return {"year": year, "format": "by_report"}

    async def download_all_custom_fields(self, year: str, variables_list: list[str]):
        """Download all custom variables from the EIA 176 custom report to a CSV.

        Args:
            year: year of the partition
            variables_list: A list containing the shortcodes for all variables in the
                custom form.
        """
        csv_name = f"eia176_{year}_custom.csv"
        dataframes = []

        for i in range(0, len(variables_list), 20):
            self.logger.debug(f"Getting variables {i}-{i + 20} of data for {year}")
            # Chunk variables list into 20 to avoid error message
            download_url = self.data_url + f"{year}/{year}/ICA/Name/"
            variables = variables_list[i : i + 20]
            for variable in variables:
                download_url += f"{variable}/"
            download_url = download_url.rstrip("/")  # Drop trailing slash

            user_agent = self.get_user_agent()
            json_response = await self.get_json(
                download_url, headers={"User-Agent": user_agent}
            )  # Generate a random user agent
            # Get data into dataframes
            try:
                dataframes.append(
                    pd.DataFrame.from_dict(json_response["data"], orient="columns")
                )
            except Exception as ex:
                raise AssertionError(
                    f"{ex}: Error processing dataframe for {year} - see {download_url}."
                )
            await asyncio.sleep(5)  # Add sleep to prevent user-agent blocks

        self.logger.info(f"Compiling data for {year}")
        dataframe = pd.concat(dataframes)

        # Rename columns. Instead of using year for value column, rename "value"
        column_dict = {
            variable["field"]: (
                str(variable["headerName"]).lower()
                if str(variable["headerName"]).lower() != year
                else "value"
            )
            for variable in json_response["columns"]
        }
        dataframe = dataframe.rename(columns=column_dict).sort_values(
            ["company", "item"]
        )

        # Convert to CSV in-memory and write to .zip with stable hash
        csv_data = dataframe.to_csv(
            encoding="utf-8",
            index=False,
        )
        return csv_name, csv_data

    async def get_year_reports_and_custom_resource(
        self,
        year: str,
        reports_list: list[EIANaturalGasData],
        variables_list: list[str],
    ) -> ResourceInfo:
        """Download all available data for a year with multiple reports.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.
        We also grab all custom fields from the custom report.

        Args:
            year: the year we're downloading data for
            reports_list: a list containing metadata items for each report to be
                downloaded
            variables_list: a list containing the shortcodes for all the variables
                in the custom form.
        """
        archive_path = self.download_directory / f"{self.name}-{year}.zip"
        data_paths_in_archive = set()

        # Start by archiving all of the pre-defined reports for a given year
        for report in reports_list:
            # If this is a valid year for this report
            # and skipping the custom data form (RPC)
            if (
                int(year) in [year.ayear for year in report.available_years]
                and report.report_code != "RPC"
            ):
                # Get a string interpretable name for the report
                report_name = (
                    report.description.replace("176", "").replace(" ", "_").lower()
                )
                # Construct the file name
                csv_name = f"{self.name}_{year}{report_name}.csv"

                # Download the data
                download_url = (
                    self.base_url + f"/{report.report_code}/data/{year}/{year}/ICA/Name"
                )

                self.logger.info(f"Retrieving data for {year} {report.report_code}")
                json_response = await self.get_json(download_url)
                dataframe = pd.DataFrame.from_dict(
                    json_response["data"], orient="columns"
                )

                if dataframe.empty and report.report_code == "RP6":
                    # The company list is expected to not have data in a known number of years.
                    # Skip uploading an empty file for these known years.
                    assert year in ["2000", "2001", "2019"]
                    self.logger.warn(f"No data found for {year} {report.report_code}")
                    continue

                # Rename columns from weirdly-formatted JSON response
                column_dict = {
                    variable["field"]: self.clean_header_name(
                        str(variable["headerName"])
                    )
                    for variable in json_response["columns"]
                    if "children" not in variable
                } | {  # Handle nested columns from JSON response
                    child["field"]: self.clean_header_name(str(child["headerName"]))
                    for variable in json_response["columns"]
                    if "children" in variable
                    for child in variable["children"]
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
                data_paths_in_archive.add(csv_name)
                await asyncio.sleep(5)  # Respect server limits

        # Now get all data variables available through the custom form for this year
        csv_name, csv_data = await self.download_all_custom_fields(year, variables_list)

        with zipfile.ZipFile(
            archive_path,
            "a",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            add_to_archive_stable_hash(
                archive=archive, filename=csv_name, data=csv_data
            )
        data_paths_in_archive.add(csv_name)

        # Finally, return the zipfile information
        partitions = await self.get_year_partitions(year)

        return ResourceInfo(
            local_path=archive_path,
            partitions=partitions,
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

"""Download EIA 176 data.

Unlike the other NGQV datasets, the "reports" provided for EIA 176 do not contain all
fields of data. To get around this, we query the list of item codes for 176 from the
portal, and then manually download subsets of data from all item codes for all years.
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
    items_url = "https://www.eia.gov/naturalgas/ngqs/data/items"
    bulk_url = "https://www.eia.gov/naturalgas/ngqs/all_ng_data.zip"

    async def get_items(self, url: str = items_url) -> list[str]:
        """Get list of item codes from EIA NQGV portal."""
        self.logger.info("Getting list of items in Form 176.")
        items_response = await self.get_json(url)
        items_list = [item["item"] for item in items_response]
        return items_list

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 176 resources, including all fields from the custom report."""
        # First grab the bulk data
        yield self.get_bulk_resource()

        # Then archive the already-defined reports
        datasets_list = await self.get_datasets(url=self.base_url, form=self.form)

        # For the custom fields, we want to ensure we're querying all possible
        # years of data availability across all EIA forms.
        # We do this by updating the all_dataset_years to include all possible years
        # covered by all other reports.
        all_dataset_years = set()

        for dataset in datasets_list:
            # Get all available years
            if dataset.code != "RPC":  # We handle the custom report separately
                dataset_years = [year.ayear for year in dataset.available_years]
                all_dataset_years.update(
                    dataset_years
                )  # Update the global list of years

        # Now, grab the data from the custom report
        items_list = await self.get_items(url=self.items_url)

        for year in all_dataset_years:
            yield self.get_year_reports_and_custom_resource(
                str(year), datasets_list, items_list
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

    async def download_all_custom_fields(self, year: str, items_list: list[str]):
        """Download all custom items from the EIA 176 custom report to a CSV."""
        csv_name = f"eia176_{year}_custom.csv"
        dataframes = []

        for i in range(0, len(items_list), 20):
            self.logger.debug(f"Getting items {i}-{i + 20} of data for {year}")
            # Chunk items list into 20 to avoid error message
            download_url = self.data_url + f"{year}/{year}/ICA/Name/"
            items = items_list[i : i + 20]
            for item in items:
                download_url += f"{item}/"
            download_url = download_url[:-1]  # Drop trailing slash

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
            item["field"]: (
                str(item["headerName"]).lower()
                if str(item["headerName"]).lower() != year
                else "value"
            )
            for item in json_response["columns"]
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
        self, year: str, datasets_list: list[EIANaturalGasData], items_list: list[str]
    ) -> ResourceInfo:
        """Download all available data for a year with multiple reports.

        We do this by constructing the URL based on the EIANaturalGasData object,
        getting the JSON and transforming it into a csv that is then zipped.
        We also grab all custom fields from the custom report.

        Args:
            year: the year we're downloading data for
            dataset: the report we're downloading
        """
        archive_path = self.download_directory / f"{self.name}-{year}.zip"
        data_paths_in_archive = set()

        for dataset in datasets_list:
            # If this is a valid year for this dataset
            # and skipping the custom data form
            if (
                int(year) in [year.ayear for year in dataset.available_years]
                and dataset.code != "RPC"
            ):
                csv_name = f"{self.name}_{year}_{dataset.code.lower()}.csv"

                download_url = (
                    self.base_url + f"/{dataset.code}/data/{year}/{year}/ICA/Name"
                )

                self.logger.info(f"Retrieving data for {year} {dataset.code}")
                json_response = await self.get_json(download_url)
                dataframe = pd.DataFrame.from_dict(
                    json_response["data"], orient="columns"
                )

                # Rename columns
                column_dict = {
                    item["field"]: str(item["headerName"])
                    .lower()
                    .replace("<br>", "_")
                    .replace(" ", "_")
                    for item in json_response["columns"]
                    if "children" not in item
                } | {  # Handle children
                    child["field"]: str(child["headerName"])
                    .lower()
                    .replace("<br>", "_")
                    .replace(" ", "_")
                    for item in json_response["columns"]
                    if "children" in item
                    for child in item["children"]
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
                await asyncio.sleep(5)  # Avoid getting cut off

        # Now handle custom form data
        csv_name, csv_data = await self.download_all_custom_fields(year, items_list)

        with zipfile.ZipFile(
            archive_path,
            "a",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            add_to_archive_stable_hash(
                archive=archive, filename=csv_name, data=csv_data
            )
        data_paths_in_archive.add(csv_name)

        # Finally, write the zipfile
        partitions = await self.get_year_partitions(year)

        return ResourceInfo(
            local_path=archive_path,
            partitions=partitions,
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

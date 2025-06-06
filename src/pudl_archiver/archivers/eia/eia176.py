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
from pudl_archiver.archivers.eia.naturalgas import EiaNGQVArchiver
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash


class Eia176Archiver(EiaNGQVArchiver):
    """EIA 176 archiver."""

    name = "eia176"
    form = "176"
    data_url = "https://www.eia.gov/naturalgas/ngqs/data/report/RPC/data/"
    items_url = "https://www.eia.gov/naturalgas/ngqs/data/items"

    async def get_items(self, url: str = items_url) -> list[str]:
        """Get list of item codes from EIA NQGV portal."""
        self.logger.info("Getting list of items in Form 176.")
        items_response = await self.get_json(url)
        items_list = [item["item"] for item in items_response]
        return items_list

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 176 resources."""
        items_list = await self.get_items(url=self.items_url)
        # Get the list of 176 datasets and their years of availability from the
        # dataset list in the NGQV API. Unlike other NGQV datasets, we query the base
        # URL only to get the list of years that data is available. The data itself is
        # downloaded from the data_url.
        datasets_list = await self.get_datasets(url=self.base_url, form=self.form)
        dataset_years = set()
        for dataset in datasets_list:
            # Get all years for which there is 176 data available, based on information
            # provided by the NGQV API.
            dataset_years.update([year.ayear for year in dataset.available_years])
        for year in dataset_years:
            year = str(year)
            yield self.get_year_resource(year, items_list=items_list)

    async def get_year_resource(self, year: str, items_list: list[str]) -> ResourceInfo:
        """Download all available data for a year.

        We do this by getting the list of all items available for Form 176,
        iteratively calling them into the URL, getting the JSON and transforming it into
        a csv that is then zipped.

        Args:
            year: the year we're downloading data for
        """
        archive_path = self.download_directory / f"eia176-{year}.zip"
        csv_name = f"eia176_{year}.csv"

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

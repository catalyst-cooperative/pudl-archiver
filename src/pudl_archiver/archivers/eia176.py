"""Download EIA 176 data."""
import logging
import zipfile

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Eia176Archiver(AbstractDatasetArchiver):
    """EIA 176 archiver."""

    name = "eia176"
    base_url = "https://www.eia.gov/naturalgas/ngqs/data/report/RPC/data/"
    items_url = "https://www.eia.gov/naturalgas/ngqs/data/items"

    async def get_items(self, url: str = items_url) -> list[str]:
        """Get list of item codes from EIA NQGS portal."""
        items_response = await self.get_json(url)
        items_list = [item["item"] for item in items_response]
        return items_list

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 176 resources."""
        items_list = await self.get_items(url=self.items_url)
        for year in range(1997, 2023):
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
            logger.debug(f"Getting items {i}-{i+20} of data for {year}")
            # Chunk items list into 20 to avoid error message
            download_url = self.base_url + f"{year}/{year}/ICA/Name/"
            items = items_list[i : i + 20]
            for item in items:
                download_url += f"{item}/"
            download_url = download_url[:-1]  # Drop trailing slash

            json_response = await self.get_json(download_url)
            # Get data into dataframes
            try:
                dataframes.append(
                    pd.DataFrame.from_dict(json_response["data"], orient="columns")
                )
            except Exception as ex:
                logger.warning(
                    f"{ex}: skipping dataframe for {year} - see {download_url}"
                )

        logger.info(f"Compiling data for {year}")
        dataframe = pd.concat(dataframes)

        # Rename columns. Instead of using year for value column, rename "value"
        column_dict = {
            item["field"]: str(item["headerName"]).lower()
            if str(item["headerName"]).lower() != year
            else "value"
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

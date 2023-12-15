"""Download EPACEMS data."""
import json
import logging
import os
import re
from pathlib import Path

import pandas as pd
import requests

from pudl_archiver.archivers import validate
from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import DataPackage, ZipLayout

logger = logging.getLogger(f"catalystcoop.{__name__}")


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"
    concurrency_limit = 2  # Number of files to concurrently download

    base_url = "https://api.epa.gov/easey/bulk-files/"
    parameters = {"api_key": os.environ["EPACEMS_API_KEY"]}  # Set to API key

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CEMS resources."""
        file_list = requests.get(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=self.parameters,
            timeout=300,
        )
        if file_list.status_code == 200:
            resjson = file_list.content.decode("utf8").replace("'", '"')
            file_list.close()  # Close connection.
            bulk_files = json.loads(resjson)
            quarterly_emissions_files = [
                file
                for file in bulk_files
                if (file["metadata"]["dataType"] == "Emissions")
                and (file["metadata"]["dataSubType"] == "Hourly")
                and ("quarter" in file["metadata"])
                and (self.valid_year(file["metadata"]["year"]))
            ]
            logger.info(f"Downloading {len(quarterly_emissions_files)} total files.")
            for i, cems_file in enumerate(quarterly_emissions_files):
                yield self.get_year_quarter_resource(
                    file=cems_file, request_count=i + 1
                )  # Count files starting at 1 for human legibility.
        else:
            raise AssertionError(
                f"EPACEMS API request did not succeed: {file_list.status_code}"
            )

    async def get_year_quarter_resource(
        self, file: dict[str, str | dict[str, str]], request_count: int | None
    ) -> tuple[Path, dict]:
        """Download all available data for a single quarter in a year.

        Args:
            file: a dictionary containing file characteristics from the EPA API.
                See https://www.epa.gov/power-sector/cam-api-portal#/swagger/camd-services
                for expected format of dictionary.
            request_count: the number of the request.
        """
        url = self.base_url + file["s3Path"]
        year = file["metadata"]["year"]
        quarter = file["metadata"]["quarter"]

        # Useful to debug at download time-outs.
        logger.debug(f"Downloading {year} Q{quarter} EPACEMS data.")

        # Create zipfile to store year/quarter combinations of files
        filename = f"epacems-{year}-{quarter}.csv"
        archive_path = self.download_directory / f"epacems-{year}-{quarter}.zip"
        # Override the default asyncio timeout to 14 minutes, just under the API limit.
        await self.download_and_zip_file(
            url=url, filename=filename, archive_path=archive_path, timeout=60 * 14
        )
        logger.info(  # Verbose but helpful to track progress
            f"File no. {request_count}: Downloaded {year} Q{quarter} EPA CEMS hourly emissions data."
        )
        return ResourceInfo(
            local_path=archive_path,
            partitions={"year": year, "quarter": quarter},
            layout=ZipLayout(filepaths=[filename]),
        )

    def dataset_validate_archive(
        self,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
    ) -> list[validate.DatasetSpecificValidation]:
        """Look for consecutive year quarter partitions in CEMS archive."""
        year_quarters = [x.parts["year_quarter"] for x in new_datapackage.resources]
        yq_split = [x.split("q") for x in year_quarters]
        yq_df = (
            pd.DataFrame(yq_split, columns=["year", "quarter"])
            .apply(pd.to_numeric, errors="coerce")
        )
        newest_year = yq_df.year.max()
        success=True
        description=""

        # Test that there are no extraneous quarter values like q5
        if not yq_df["quarter"].isin(range(1,5)).all():
            success=False
            description = description + f"""
                f"The following years have bad quarters: \
                {yq_df[yq_df['quarter'].isin(range(1,5))].year.unique().tolist()}. """
        # Test that all old years have all four quarters
        q_groups = yq_df[yq_df["year"]!=newest_year].groupby("year").count()
        if not q_groups.quarter.isin([4]).all():
            success=False
            description = description + f"""
                f"The following years do not have four quarters: \
                    {q_groups[q_groups['quarter']!=4].index.tolist()}. """
        # Test that the newest year has consecutive quarters
        new_quarters = yq_df[yq_df["year"]==newest_year].quarter.unique().tolist()
        if len(set(range(1,len(new_quarters)+1)) - set(new_quarters)) != 0:
            success=False
            description = description + "There are missing quarters in the new year of data. "

        return {
            "name": "dataset_validate_archive",
            "description": description,
            "success": success,
        }
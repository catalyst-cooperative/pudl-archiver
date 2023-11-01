"""Download EPACEMS data."""
import json
import logging
import os
from pathlib import Path

import requests

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"
    concurrency_limit = 5  # Number of files to concurrently download

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
                and ("quarter" in file["metadata"].keys())
                and (self.valid_year(file["metadata"]["year"]))
            ]
            logger.info(f"Downloading {len(quarterly_emissions_files)} total files.")
            # For each year, download all quarters and zip into one file.
            years = list(
                {file["metadata"]["year"] for file in quarterly_emissions_files}
            )
            for year in years:
                cems_files = [
                    file
                    for file in quarterly_emissions_files
                    if file["metadata"]["year"] == year
                ]
                yield self.get_year_resource(files=cems_files, year=year)
        else:
            raise AssertionError(
                f"EPACEMS API request did not succeed: {file_list.status_code}"
            )

    async def get_year_resource(
        self, files: list[dict[str, str | dict[str, str]]], year: int
    ) -> tuple[Path, dict]:
        """Download all available data for a single quarter in a year.

        Args:
            files: a list of dictionaries containing file characteristics from the EPA
                API. See
                https://www.epa.gov/power-sector/cam-api-portal#/swagger/camd-services
                for expected format of dictionary.
            request_count: the number of the request.
            year: year of data to download.
        """
        logger.info(f"Downloading and zipping {year} CEMS data.")
        files_dict = {}
        archive_path = self.download_directory / f"epacems-{year}.zip"
        for i, file in enumerate(files):
            files_dict[i] = {}
            quarter = file["metadata"]["quarter"]
            files_dict[i]["url"] = self.base_url + file["s3Path"]
            files_dict[i]["year"] = file["metadata"]["year"]
            files_dict[i]["quarter"] = quarter
            files_dict[i]["filename"] = f"epacems-{year}-{quarter}.csv"
            files_dict[i]["archive_path"] = archive_path

        # Create zipfile to store year/quarter combinations of files
        # Override the default asyncio timeout to 14 minutes, just under the API limit.
        await self.download_and_zip_files(files_dict=files_dict, timeout=60 * 14)

        return ResourceInfo(local_path=archive_path, partitions={"year": year})

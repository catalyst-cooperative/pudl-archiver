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
            for i, cems_file in enumerate(quarterly_emissions_files):
                yield self.get_quarter_year_resource(file=cems_file, request_count=i)
        else:
            raise AssertionError(
                f"EPACEMS API request did not succeed: {file_list.status_code}"
            )

    async def get_quarter_year_resource(
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
        logger.debug(f"Downloading Q{quarter} {year} EPACEMS data.")

        # Create zipfile to store year/quarter combinations of files
        filename = f"epacems-{year}-{quarter}.csv"
        archive_path = self.download_directory / f"epacems-{year}-{quarter}.zip"
        # Override the default asyncio timeout to 14 minutes, just under the API limit.
        await self.download_and_zip_file(
            url=url, filename=filename, archive_path=archive_path, timeout=60 * 14
        )
        logger.info(  # Verbose but helpful to keep track of progress.
            f"File no. {request_count}: Downloaded Q{quarter} {year} EPA CEMS hourly emissions data."
        )
        return ResourceInfo(
            local_path=archive_path, partitions={"year": year, "quarter": quarter}
        )

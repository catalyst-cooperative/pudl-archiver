"""Download EPACEMS data."""
import asyncio
import json
import logging
import os
from pathlib import Path

import numpy as np
import requests

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")
STATE_ABBREVIATIONS = [
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "dc",
    "de",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "pr",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
]


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"
    concurrency_limit = 10  # Number of files to concurrently download
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
            hourly_state_emissions_files = [
                file
                for file in bulk_files
                if (file["metadata"]["dataType"] == "Emissions")
                and (file["metadata"]["dataSubType"] == "Hourly")
                and ("stateCode" in file["metadata"].keys())
            ]
            logger.info(
                f"Downloading {len(hourly_state_emissions_files)} total files. This will take more than one hour to respect API rate limits."
            )
            if len(hourly_state_emissions_files) > 1000:
                # This could be used to add pauses to later requests, but in practice
                # read time is so slow that this takes several hours with concurrency.
                # If this changes, the request_counter could be used to add a delay.
                request_counter = 0
            else:
                request_counter = np.nan
            for cems_file in hourly_state_emissions_files:
                if request_counter is not None:
                    request_counter += 1
                if self.valid_year(cems_file["metadata"]["year"]):
                    yield self.get_state_year_resource(
                        file=cems_file, request_count=request_counter
                    )

    async def get_state_year_resource(
        self, file: dict[str, str | dict[str, str]], request_count: int | None
    ) -> tuple[Path, dict]:
        """Download all available data for a single state/year.

        Args:
            file: a dictionary containing file characteristics from the EPA API.
            request_count: the number of the request.
        """
        url = self.base_url + file["s3Path"]
        year = file["metadata"]["year"]
        state = file["metadata"]["stateCode"].lower()
        file_size = file["megaBytes"]
        if int(file_size) > 600:  # If bigger than 600 mb
            await asyncio.sleep(60 * 5)
            # Add a five-minute wait time for very big files to let
            # other files in group finish first.

        logger.info(f"Downloading EPACEMS data for {state.upper()}, {year}")
        # Create zipfile to store year/state combinations of files
        filename = f"epacems-{year}-{state}.csv"
        archive_path = self.download_directory / f"epacems-{year}-{state}.zip"
        await self.download_and_zip_file(
            url=url, filename=filename, archive_path=archive_path, timeout=60 * 14
        )
        # Override the default asyncio timeout to 14 minutes, just under the API limit.
        logger.info(
            f"File no. {request_count}: Downloaded {year} EPACEMS data for {state.upper()}"
        )
        return ResourceInfo(
            local_path=archive_path, partitions={"year": year, "state": state}
        )

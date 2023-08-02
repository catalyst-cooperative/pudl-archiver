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
BASE_URL = "https://api.epa.gov/easey/bulk-files/"
parameters = {"api_key": os.environ["EPACEMS_API_KEY"]}  # Set to API key


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"
    concurrency_limit = 10  # Number of files to concurrently download

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        file_list = requests.get(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=parameters,
            timeout=300,
        )
        if file_list.status_code == 200:
            resjson = file_list.content.decode("utf8").replace("'", '"')
            file_list.close()  # Close connection.
            bulk_files = json.loads(resjson)
            hourly_emissions_files = [
                file
                for file in bulk_files
                if (file["metadata"]["dataType"] == "Emissions")
                and (file["metadata"]["dataSubType"] == "Hourly")
            ]
            hourly_state_emissions_files = [
                file
                for file in hourly_emissions_files
                if "stateCode" in file["metadata"].keys()
            ]
            logger.info(
                f"Downloading {len(hourly_state_emissions_files)} total files. This will take more than one hour to respect API rate limits."
            )
            if len(hourly_state_emissions_files) > 1000:
                # Implement request counting to avoid rate limit of 1000 requests/hour
                request_counter = 0
            else:
                request_counter = np.nan
            for cems_file in hourly_state_emissions_files:
                if request_counter is not None:
                    request_counter += 1
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
        url = BASE_URL + file["s3Path"]
        year = file["metadata"]["year"]
        state = file["metadata"]["stateCode"].lower()
        # file_size = file["megaBytes"]
        download_path = self.download_directory / f"epacems-{year}-{state}.csv"
        if (
            request_count > 950
        ):  # Give a bit of buffer room for the 1000 requests per hour limit
            await asyncio.sleep(60 * 60)
        await self.download_file(url=url, file=download_path, timeout=60 * 14)
        # Override the default asyncio timeout to 14 minutes, just under the API limit.
        logger.info(f"Downloaded {year} EPACEMS data for {state.upper()}")
        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "state": state}
        )

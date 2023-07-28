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

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        file_list = requests.get(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=parameters,
            timeout=5,
        )  # FIX ME to use existing classes
        resjson = file_list.content.decode("utf8").replace("'", '"')
        bulk_files = json.loads(resjson)
        hourly_emissions_files = [
            file
            for file in bulk_files
            if (file["metadata"]["dataType"] == "Emissions")
            and (file["metadata"]["dataSubType"] == "Hourly")
        ]
        for file in hourly_emissions_files:
            if "stateCode" in file["metadata"].keys():
                url = BASE_URL + file["s3Path"]
                year = file["metadata"]["year"]
                state = file["metadata"]["stateCode"].lower()
                # logger.info(url)
                yield self.get_state_year_resource(year=year, state=state, url=url)

    async def get_state_year_resource(
        self, year: int, state: str, url: str
    ) -> tuple[Path, dict]:
        """Download all available data for a single state/year."""
        logger.info(f"Downloading EPACEMS data for {state.upper()}, {year}")
        download_path = self.download_directory / f"epacems-{year}-{state}.csv"

        await self.download_file(url, download_path)
        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "state": state}
        )

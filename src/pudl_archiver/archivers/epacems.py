"""Download EPACEMS data."""
import logging
import os
from pathlib import Path

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

BASE_URL = "https://api.epa.gov/easey/bulk-files/emissions/hourly/state/"
parameters = {"api_key": os.environ["EPACEMS_API_KEY"]}  # Set to API key


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        for state in STATE_ABBREVIATIONS:
            for year in range(1995, 2023):
                yield self.get_state_year_resource(year=year, state=state)

    async def get_state_year_resource(self, year: int, state: str) -> tuple[Path, dict]:
        """Download all available data for a single state/year."""
        logger.info(f"Downloading EPACEMS data for {state.capitalize()}, {year}")

        url = f"{BASE_URL}/emissions-hourly-{year}-{state}.csv"
        download_path = self.download_directory / f"epacems-{year}-{state}.csv"

        await self.download_file(url, download_path)
        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "state": state}
        )

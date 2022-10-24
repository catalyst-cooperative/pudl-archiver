"""Download EPACEMS data."""
import io
import logging
import re
import zipfile
from pathlib import Path

from pudl_archiver.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable

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
BASE_URL = "https://gaftp.epa.gov/DMDnLoad/emissions/hourly/monthly"


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        year_pattern = re.compile(r"\d{4}\/")
        file_pattern = re.compile(r"(\d{4})([a-z]{2})([0-1][0-9])\.zip")

        # Loop through all available years of data
        for year in await self.get_hyperlinks(BASE_URL, year_pattern, verify=False):
            year = int(year[:-1])
            # Store months available for each state
            states = {state: [] for state in STATE_ABBREVIATIONS}
            for link in await self.get_hyperlinks(
                f"{BASE_URL}/{year}", file_pattern, verify=False
            ):
                match = file_pattern.search(link)
                states[match.group(2)].append(int(match.group(3)))

            for state, months in states.items():
                if len(months) > 0:
                    yield self.get_state_year_resource(year, state, months)

    async def get_state_year_resource(
        self, year: int, state: str, months: list[int]
    ) -> tuple[Path, dict]:
        """Download all available months of data for a single state/year."""
        logger.info(f"Downloading EPACEMS data for {state.capitalize()}, {year}")
        # Create zipfile to store year/state combinations of files
        archive_path = self.download_directory / f"epacems-{year}-{state}.zip"

        for month in months:
            filename = f"{year}{state}{month:02}.zip"
            url = f"{BASE_URL}/{year}/{filename}"

            with io.BytesIO() as f_memory:
                await self.download_zipfile(url, f_memory, ssl=False)

                # Write to zipfile
                with zipfile.ZipFile(
                    archive_path, "a", compression=zipfile.ZIP_DEFLATED
                ) as archive:
                    with archive.open(filename, "w") as f_disk:
                        f_disk.write(f_memory.read())

        return archive_path, {"year": year, "state": state}

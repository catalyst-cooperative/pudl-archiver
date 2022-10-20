"""Download EPACMES data."""
import io
import logging
import re
import zipfile
from html.parser import HTMLParser
from pathlib import Path

import requests

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable

logger = logging.getLogger(f"catalystcoop.{__name__}")
FILE_PATTERN = re.compile(r"(\d{4})([a-z]{2})([0-1][0-9])\.zip")
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


class EpaCemsParser(HTMLParser):
    """Implements an ultra minimal HTML parser to extract month/state combos available."""

    def __init__(self):
        super().__init__()
        self.available_partitions = {state: [] for state in STATE_ABBREVIATIONS}

    def handle_data(self, data):
        """Check if data matches filename structure and add to partitions."""
        pattern_match = FILE_PATTERN.match(data)

        if pattern_match:
            state = pattern_match.group(2)
            month = int(pattern_match.group(3))

            if state not in STATE_ABBREVIATIONS:
                logger.warning("Invalid state in file: {data}")
            else:
                self.available_partitions[state].append(month)


class EpaCemsArchiver(AbstractDatasetArchiver):
    name = "epacems"

    def get_resources(self) -> ArchiveAwaitable:
        """Download EIA bulk electricity resources."""
        for year in range(1995, self.current_year()):
            parser = EpaCemsParser()
            year_page = requests.get(f"{BASE_URL}/{year}", verify=False)
            parser.feed(year_page.text)

            # Loop through available month/state combinations
            for state, months in parser.available_partitions.items():
                if len(months) > 0:
                    yield self.get_state_year_resource(year, state, months)

    async def get_state_year_resource(
        self, year: int, state: str, months: list[int]
    ) -> tuple[Path, dict]:
        """Download all available months of data for a single state/year."""
        # Create directory to store year/state combinations of files
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

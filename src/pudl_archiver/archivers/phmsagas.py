"""Download PHMSHA data."""
import re
import typing
from datetime import date
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids"
current_year = date.today().year


class PhmsaGasArchiver(AbstractDatasetArchiver):
    """PHMSA Gas Annual Reports archiver."""

    name = "phmsagas"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download PHMSA gas resources."""
        link_pattern = re.compile(r"annual[-|_](\S+).zip")

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_zip_resource(link, link_pattern.search(link))

    async def get_zip_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file."""
        # Use archive link if year is not most recent year

        url = f"https://www.phmsa.dot.gov/{link}"
        file = str(match.group(1)).replace("_", "-")  # Get file name

        # Set data_type partition
        if "distribution" in file:
            data_type = "distribution"
        elif "transmission" in file:
            data_type = "transmission"
        elif "liquefied" in file:
            data_type = "LNG"
        elif "hazardous" in file:
            data_type = "hazliq"
        elif "underground" in file:
            data_type = "UNGS"
        else:
            data_type = None

        # Set start and end years
        start_year = int(file.split("-")[-2])
        end_year = file.split("-")[-1]
        if "present" in end_year:
            end_year = current_year

        download_path = self.download_directory / f"phmsagas-{file}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={
                "start_year": start_year,
                "end_year": end_year,
                "data_type": data_type,
            },
        )

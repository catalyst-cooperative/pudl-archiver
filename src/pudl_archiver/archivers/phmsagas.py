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
        url = f"https://www.phmsa.dot.gov/{link}"
        file = str(match.group(1)).replace("-", "_")  # Get file name

        # Set dataset partition
        dataset = "_".join(file.lower().split("_")[0:-2])

        # Set start year
        start_year = int(file.split("_")[-2])

        download_path = self.download_directory / f"phmsagas_{file}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={
                "start_year": start_year,
                "dataset": dataset,
            },
        )

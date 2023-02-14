"""Download PHMSHA data."""
import logging
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")

BASE_URL = "https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids"

PHMSA_DATASETS = [
    "underground_natural_gas_storage",
    "liquefied_natural_gas",
    "hazardous_liquid",
    "gas_transmission_gathering",
    "gas_distribution",
]


class PhmsaGasArchiver(AbstractDatasetArchiver):
    """PHMSA Gas Annual Reports archiver."""

    name = "phmsagas"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download PHMSA gas resources."""
        link_pattern = re.compile(r"annual[-|_](\S+).zip")

        # Get main table links.
        links = await self.get_hyperlinks(BASE_URL, link_pattern)
        datasets = [
            "_".join(
                link_pattern.search(link)
                .group(1)
                .lower()
                .replace("-", "_")
                .split("_")[0:-2]
            )
            for link in links
        ]

        # Raise error if any expected dataset missing.
        if not all(item in datasets for item in PHMSA_DATASETS):
            missing_data = ", ".join(
                [item for item in PHMSA_DATASETS if item not in datasets]
            )
            raise ValueError(
                f"Expected dataset download links not found for datasets: {missing_data}"
            )

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_zip_resource(link, link_pattern.search(link))

    async def get_zip_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file.

        Filenames generally look like: annual_{dataset}_{start_year}_{end_year}.zip
        For example: annual_underground_natural_gas_storage_2017_present.zip
        """
        url = f"https://www.phmsa.dot.gov/{link}"
        file = str(match.group(1)).replace("-", "_")  # Get file name

        # Set dataset partition
        dataset = "_".join(file.lower().split("_")[0:-2])

        if dataset not in PHMSA_DATASETS:
            logger.warning(f"New dataset type found: {dataset}.")

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

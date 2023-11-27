"""Download PHMSHA data."""
import logging
import re
import typing
from datetime import datetime
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")

BASE_URL = "https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids"

PHMSA_FORMS = [
    "underground_natural_gas_storage",
    "liquefied_natural_gas",
    "hazardous_liquid",
    "gas_transmission_gathering",
    "gas_distribution",
    "reporting_regulated_gas_gathering",
]


class PhmsaGasArchiver(AbstractDatasetArchiver):
    """PHMSA Gas Annual Reports archiver."""

    name = "phmsagas"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download PHMSA gas resources."""
        link_pattern = re.compile(r"annual[-|_](\S+).zip")

        # Get main table links.
        links = await self.get_hyperlinks(BASE_URL, link_pattern)
        forms = [
            "_".join(
                link_pattern.search(link)
                .group(1)
                .lower()
                .replace("-", "_")
                .split("_")[0:-2]
            )
            for link in links
        ]

        # Raise error if any expected form missing.
        if not all(item in forms for item in PHMSA_FORMS):
            missing_data = ", ".join(
                [item for item in PHMSA_FORMS if item not in forms]
            )
            raise ValueError(
                f"Expected form download links not found for forms: {missing_data}"
            )

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_zip_resource(link, link_pattern.search(link))

    async def get_zip_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file.

        Filenames generally look like: annual_{form}_{start_year}_{end_year}.zip
        For example: annual_underground_natural_gas_storage_2017_present.zip
        """
        url = f"https://www.phmsa.dot.gov/{link}"
        filename = str(match.group(1)).replace("-", "_")  # Get file name

        # Set form partition
        form = "_".join(filename.lower().split("_")[0:-2])

        if form not in PHMSA_FORMS:
            logger.warning(f"New form type found: {form}.")

        # From start and end year, get partitions
        start_year = int(filename.split("_")[-2])
        end_year = filename.split("_")[-1]
        # If end year is present, this will include data through last year.
        end_year = datetime.now().year if end_year == "present" else int(end_year)
        years = list(range(start_year, end_year))

        download_path = self.download_directory / f"{self.name}-{filename}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={
                "years": years,
                "form": form,
            },
        )

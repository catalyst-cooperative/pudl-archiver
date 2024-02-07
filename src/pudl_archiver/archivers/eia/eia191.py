"""Download EIA 191 data."""
import logging

from pudl_archiver.archivers.classes import (
    ArchiveAwaitable,
)
from pudl_archiver.archivers.eia.naturalgas import EiaNGQVArchiver

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Eia191Archiver(EiaNGQVArchiver):
    """EIA 191 archiver."""

    name = "eia191"
    form = "191"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA 191 resources, keeping monthly data only."""
        datasets_list = await self.get_datasets(url=self.base_url, form=self.form)

        for dataset in datasets_list:
            if "Monthly" in dataset.description:  # Archive monthly, not annual data
                # Get all available years
                dataset_years = [year.ayear for year in dataset.available_years]
                for year in dataset_years:
                    yield self.get_year_resource(year, dataset)
            else:
                logger.info(f"Skipping archiving {dataset.description}")

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
        reports_list = await self.get_reports(url=self.base_url, form=self.form)

        for report in reports_list:
            if "Monthly" in report.description:  # Archive monthly, not annual data
                # Get all available years
                report_years = [year.ayear for year in report.available_years]
                for year in report_years:
                    yield self.get_year_resource(year, report)
            else:
                logger.info(f"Skipping archiving {report.description}")

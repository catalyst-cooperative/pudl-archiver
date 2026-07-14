"""Defines base class for archiver."""

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
)
from pudl_archiver.archivers.ferc import ferc_online_helpers, xbrl


class Ferc2Archiver(AbstractDatasetArchiver):
    """Ferc Form 2 archiver."""

    name = "ferc2"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 2 resources."""
        dbf_years = [year for year in range(1994, 2022) if self.valid_year(year)]
        yield ferc_online_helpers.get_resources_for_form(
            ferc_form="2",
            years=dbf_years,
            partitions_base={"data_format": "DBF"},
            download_directory=self.download_directory,
        )

        # Get XBRL filings
        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_2,
            self.download_directory,
            self.valid_year,
            self.session,
        )

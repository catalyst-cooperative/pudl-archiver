"""Download FERC Form 1 data."""

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
)
from pudl_archiver.archivers.ferc import ferc_online_helpers, xbrl


class Ferc1Archiver(AbstractDatasetArchiver):
    """Ferc Form 1 archiver."""

    name = "ferc1"
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 1 resources."""
        dbf_years = [year for year in range(1994, 2022) if self.valid_year(year)]
        yield ferc_online_helpers.get_resources_for_form(
            ferc_form="1",
            years=dbf_years,
            partitions_base={"data_format": "dbf"},
            download_directory=self.download_directory,
        )

        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_1, self.download_directory, self.valid_year, self.session
        )

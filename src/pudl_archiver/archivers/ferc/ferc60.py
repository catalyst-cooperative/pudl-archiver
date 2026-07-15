"""Defines base class for archiver."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import ferc_online_helpers, xbrl


class Ferc60Archiver(AbstractDatasetArchiver):
    """Ferc Form 60 archiver."""

    name = "ferc60"
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 60 resources."""
        dbf_years = [year for year in range(2006, 2021) if self.valid_year(year)]
        yield ferc_online_helpers.get_resources_for_form(
            ferc_form="1",
            years=dbf_years,
            partitions_base={"data_format": "DBF"},
            download_directory=self.download_directory,
        )

        # Get XBRL filings
        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_60,
            self.download_directory,
            self.valid_year,
            self.session,
        )

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 60 data."""
        url = f"https://forms.ferc.gov/f60allyears/f60_{year}.zip"
        download_path = self.download_directory / f"ferc60-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "dbf"}
        )

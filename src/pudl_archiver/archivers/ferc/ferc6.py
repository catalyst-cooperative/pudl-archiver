"""Defines base class for archiver."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import xbrl


class Ferc6Archiver(AbstractDatasetArchiver):
    """FERC Form 6 archiver."""

    name = "ferc6"
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 6 resources."""
        for year in range(2000, 2022):
            if not self.valid_year(year):
                continue
            yield self.get_year_dbf(year)

        # Get XBRL filings
        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_6,
            self.download_directory,
            self.valid_year,
            self.session,
        )

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 6 data."""
        url = f"https://forms.ferc.gov/f6allyears/f6_{year}.zip"
        download_path = self.download_directory / f"ferc6-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "dbf"}
        )

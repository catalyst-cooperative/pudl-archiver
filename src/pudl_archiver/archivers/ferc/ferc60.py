"""Defines base class for archiver."""
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import xbrl


class Ferc60Archiver(AbstractDatasetArchiver):
    """Ferc Form 60 archiver."""

    name = "ferc60"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 60 resources."""
        for year in range(2006, 2021):
            if not self.valid_year(year):
                continue
            yield self.get_year_dbf(year)

        filings = xbrl.index_available_entries()[xbrl.FercForm.FORM_60]
        for year, year_filings in filings.items():
            if not self.valid_year(year):
                continue
            yield self.get_year_xbrl(year, year_filings)

    async def get_year_xbrl(
        self, year: int, filings: xbrl.FormFilings
    ) -> tuple[Path, dict]:
        """Download all XBRL filings from a single year."""
        download_path = await xbrl.archive_year(
            year, filings, xbrl.FercForm.FORM_60, self.download_directory, self.session
        )

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "xbrl"}
        )

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 60 data."""
        url = f"https://forms.ferc.gov/f60allyears/f60_{year}.zip"
        download_path = self.download_directory / f"ferc60-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "dbf"}
        )

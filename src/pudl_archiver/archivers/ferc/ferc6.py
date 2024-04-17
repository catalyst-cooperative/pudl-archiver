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

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 6 resources."""
        for year in range(2000, 2022):
            if not self.valid_year(year):
                continue
            yield self.get_year_dbf(year)

        filings = xbrl.index_available_entries()[xbrl.FercForm.FORM_6]
        taxonomy_years = []
        for year, year_filings in filings.items():
            if not self.valid_year(year):
                continue
            if year > 2019:
                taxonomy_years.append(year)
            yield self.get_year_xbrl(year, year_filings)

        if len(taxonomy_years) > 0:
            yield xbrl.archive_taxonomy(
                year, xbrl.FercForm.FORM_6, self.download_directory, self.session
            )

    async def get_year_xbrl(
        self, year: int, filings: xbrl.FormFilings
    ) -> tuple[Path, dict]:
        """Download all XBRL filings from a single year."""
        download_path = await xbrl.archive_year(
            year, filings, xbrl.FercForm.FORM_6, self.download_directory, self.session
        )

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "xbrl"}
        )

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 6 data."""
        url = f"https://forms.ferc.gov/f6allyears/f6_{year}.zip"
        download_path = self.download_directory / f"ferc6-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "dbf"}
        )

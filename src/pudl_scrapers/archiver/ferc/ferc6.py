"""Defines base class for archiver."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable
from pudl_scrapers.archiver.ferc import xbrl


class Ferc6Archiver(AbstractDatasetArchiver):
    name = "ferc6"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 6 resources."""
        for year in range(2000, 2022):
            yield self.get_year_dbf(year)

        filings = xbrl.index_available_entries()[xbrl.FercForm.FORM_6]
        for year, year_filings in filings.items():
            yield self.get_year_xbrl(year, year_filings)

    async def get_year_xbrl(
        self, year: int, filings: xbrl.FormFilings
    ) -> tuple[Path, dict]:
        """Download all XBRL filings from a single year."""
        download_path = await xbrl.archive_year(
            year, filings, xbrl.FercForm.FORM_6, self.download_directory, self.session
        )

        return download_path, {"year": year, "data_format": "XBRL"}

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 6 data."""
        url = f"https://forms.ferc.gov/f6allyears/f6_{year}.zip"
        download_path = self.download_directory / f"ferc6-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year, "data_format": "DBF"}

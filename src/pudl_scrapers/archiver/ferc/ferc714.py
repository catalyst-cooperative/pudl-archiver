"""Download FERC Form 714 data."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable
from pudl_scrapers.archiver.ferc import xbrl


class Ferc714Archiver(AbstractDatasetArchiver):
    name = "ferc714"

    def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 714 resources."""
        yield self.get_bulk_csv()

        filings = xbrl.index_available_entries()[xbrl.FercForm.FORM_714]
        for year, year_filings in filings.items():
            yield self.get_year_xbrl(year, year_filings)

    async def get_year_xbrl(
        self, year: int, filings: xbrl.FormFilings
    ) -> tuple[Path, dict]:
        """Download all XBRL filings from a single year."""
        download_path = await xbrl.archive_year(
            year, filings, xbrl.FercForm.FORM_714, self.download_directory, self.session
        )

        return download_path, {"year": year, "data_format": "XBRL"}

    async def get_bulk_csv(self) -> tuple[Path, dict]:
        """Download a single year of FERC Form 714 data."""
        url = "https://www.ferc.gov/sites/default/files/2021-06/Form-714-csv-files-June-2021.zip"
        download_path = self.download_directory / "ferc714.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {}

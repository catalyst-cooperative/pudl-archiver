"""Defines base class for archiver."""
from pathlib import Path

from pudl_archiver.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable
from pudl_archiver.archiver.ferc import xbrl


class Ferc2Archiver(AbstractDatasetArchiver):
    """Ferc Form 2 archiver."""

    name = "ferc2"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 2 resources."""
        # Get sub-annually partitioned DBF data
        for year in range(1991, 2000):
            for part in [1, 2]:
                yield self.get_year_dbf(year, part)

        # Get annually partitioned DBF data
        for year in range(1996, 2022):
            yield self.get_year_dbf(year)

        filings = xbrl.index_available_entries()[xbrl.FercForm.FORM_2]
        for year, year_filings in filings.items():
            yield self.get_year_xbrl(year, year_filings)

    async def get_year_xbrl(
        self, year: int, filings: xbrl.FormFilings
    ) -> tuple[Path, dict]:
        """Download all XBRL filings from a single year."""
        download_path = await xbrl.archive_year(
            year, filings, xbrl.FercForm.FORM_2, self.download_directory, self.session
        )

        return download_path, {"year": year, "data_format": "XBRL"}

    async def get_year_dbf(
        self, year: int, part: int | None = None
    ) -> tuple[Path, dict]:
        """Download a single year of FERC Form 2 data."""
        early_urls: dict[tuple(int, int), str] = {
            (1991, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y91A-M.zip",
            (1991, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y91N-Z.zip",
            (1992, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y92A-M.zip",
            (1992, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y92N-Z.zip",
            (1993, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y93A-M.zip",
            (1993, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y93N-Z.zip",
            (1994, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y94A-M.zip",
            (1994, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y94N-Z.zip",
            (1995, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y95A-M.zip",
            (1995, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y95N-Z.zip",
            (1996, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y96-1.zip",
            (1996, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y96-2.zip",
            (1997, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y97-1.zip",
            (1997, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y97-2.zip",
            (1998, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y98-1.zip",
            (1998, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y98-2.zip",
            (1999, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y99-1.zip",
            (1999, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y99-2.zip",
        }
        # Special rules for grabbing the early two-part data:
        if part is not None:
            assert year >= 1991 and year <= 1999  # nosec: B101
            url = early_urls[(year, part)]
            download_path = self.download_directory / f"ferc2-{year}-{part}.zip"
        else:
            assert year >= 1996 and year <= 2021  # nosec: B101
            url = f"https://forms.ferc.gov/f2allyears/f2_{year}.zip"
            download_path = self.download_directory / f"ferc2-{year}.zip"

        await self.download_zipfile(url, download_path)

        return download_path, {"year": year, "data_format": "DBF", "part": part}

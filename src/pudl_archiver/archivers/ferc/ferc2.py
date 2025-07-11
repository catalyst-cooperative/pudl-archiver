"""Defines base class for archiver."""

from pathlib import Path

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import xbrl


class Ferc2Archiver(AbstractDatasetArchiver):
    """Ferc Form 2 archiver."""

    name = "ferc2"

    async def after_download(self) -> None:
        """Clean up playwright once downloads are complete."""
        await self.browser.close()
        await self.playwright.stop()

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 2 resources."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.webkit.launch()

        # Get sub-annually partitioned DBF data
        for year in range(1991, 2000):
            if not self.valid_year(year):
                continue
            for part in [1, 2]:
                yield self.get_year_dbf(year, part)

        # Get annually partitioned DBF data
        for year in range(1996, 2022):
            if not self.valid_year(year):
                continue
            yield self.get_year_dbf(year)

        # Get XBRL filings
        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_2,
            self.download_directory,
            self.valid_year,
            self.session,
        )

    async def get_year_dbf(
        self, year: int, part: int | None = None
    ) -> tuple[Path, dict]:
        """Download a single DBF of historical FERC Form 2 data from 1991-2021.

        Source page:
            https://www.ferc.gov/industries-data/natural-gas/industry-forms/form-2-2a-3-q-gas-historical-vfp-data
        """
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
        partitions = {}
        if part is not None:
            assert year >= 1991 and year <= 1999  # nosec: B101
            partitions = {"part": part}
            url = early_urls[(year, part)]
            download_path = self.download_directory / f"ferc2-{year}-{part}.zip"
        else:
            assert year >= 1996 and year <= 2021  # nosec: B101
            url = f"https://forms.ferc.gov/f2allyears/f2_{year}.zip"
            download_path = self.download_directory / f"ferc2-{year}.zip"

        await self.download_zipfile_via_playwright(self.browser, url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions=partitions | {"year": year, "data_format": "dbf"},
        )

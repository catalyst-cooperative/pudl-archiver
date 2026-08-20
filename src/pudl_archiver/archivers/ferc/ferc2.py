"""Defines base class for archiver."""

from pathlib import Path

from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import ferc_online_helpers, xbrl


class Ferc2Archiver(AbstractDatasetArchiver):
    """Ferc Form 2 archiver."""

    name = "ferc2"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 2 resources.

        We grab these from three places:
            * 1991-1995 DBF data from the FERC Form 2 Historical VFP data page
            * 1996-2021 DBF data from the FERC Online Viewer
            * 2022-present XBRL data from the FERC RSS Feed.
        """
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.webkit.launch()

        ferc_historical_dbf_years = [
            year for year in range(1991, 1996) if self.valid_year(year)
        ]
        for year in ferc_historical_dbf_years:
            for part in [1, 2]:
                yield self.get_year_dbf(year, part)

        ferc_online_dbf_years = [
            year for year in range(1996, 2022) if self.valid_year(year)
        ]
        yield ferc_online_helpers.get_resources_for_form(
            ferc_form="2",
            years=ferc_online_dbf_years,
            partitions_base={"data_format": "dbf"},
            download_directory=self.download_directory,
        )

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
        }
        # Special rules for grabbing the early two-part data:
        partitions = {}
        if part is not None:
            assert year >= 1991 and year <= 1999  # nosec: B101
            partitions = {"part": part}
            url = early_urls[(year, part)]
            download_path = self.download_directory / f"ferc2-{year}-{part}.zip"
        else:
            raise ValueError(
                "We're no longer expecting to call this method for years with no partition."
            )

        await self.download_zipfile_via_playwright(self.browser, url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions=partitions | {"year": year, "data_format": "dbf"},
        )

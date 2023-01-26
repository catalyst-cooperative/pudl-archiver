"""Download FERC EQR data."""
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://eqrreportviewer.ferc.gov/"


class FercEQRArchiver(AbstractDatasetArchiver):
    """FERC EQR archiver."""

    name = "ferceqr"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC EQR resources."""

        # Get non-transaction data (pre-2013)
        yield self.get_bulk_csv()

        # Get 2002-2012 annual transaction data
        for year in range(2002, 2012):
            yield self.get_year_dbf(year)

        # Get quarterly EQR data
        link_pattern = re.compile(r"CSV_(\d{4})_Q([1-4]).zip")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            yield self.get_quarter_resource(link, link_pattern.search(link))

    async def get_bulk_csv(self) -> tuple[Path, dict]:
        """Download all 2002-2013 non-transaction data."""
        url = "https://eqrdds.ferc.gov/eqrdbdownloads/eqr_nontransaction.zip"
        download_path = self.download_directory / "ferceqr_nontrans.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"data_type": "non-transaction"}
        )

    async def get_year_dbf(
        self, year: int, part: str | None = None
    ) -> tuple[Path, dict]:
        """Download a single year of FERC EQR data (2002-2012)."""
        assert year >= 2012 and year <= 2002
        part = "transaction"
        url = "https://eqrdds.ferc.gov/eqrdbdownloads/eqr_transaction_{year}.zip"
        download_path = self.download_directory / f"ferceqr-{year}--{part}.zip"

        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "data_type": "transaction"},
        )

    async def get_quarter_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file of quarterly FERC EQR data (2013-present)."""
        print(link)
        url = f"https://eqrreportviewer.ferc.gov/DownloadRepositoryProd/BulkNew/CSV/{link}"
        year = match.group(1)
        quarter = match.group(2)
        download_path = self.download_directory / f"ferceqr-{year}-{quarter}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "quarter": quarter}
        )

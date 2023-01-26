"""Download FERC EQR data."""
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

        # Get 2002-2013 annual transaction data
        for year in range(2002, 2014):
            yield self.get_year_dbf(year)

        # Get quarterly EQR data
        for year in range(2014, 2023):
            for quarter in range(1, 5):
                yield self.get_year_dbf(year, quarter)

    async def get_bulk_csv(self) -> tuple[Path, dict]:
        """Download all 2002-2013 non-transaction data."""
        url = "https://eqrdds.ferc.gov/eqrdbdownloads/eqr_nontransaction.zip"
        download_path = self.download_directory / "ferceqr_nontrans.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"data_type": "non-transaction"}
        )

    async def get_year_dbf(
        self,
        year: int,
        quarter: int | None = None,
    ) -> tuple[Path, dict]:
        """Download FERC EQR transaction data (quarterly or annual).

        Download a year (2002-2013) of FERC EQR transaction data,
        or a quarter of 2014-present data.
        """
        # For 2014 - present data
        if quarter is not None:
            part = None
            url = f"https://eqrreportviewer.ferc.gov/DownloadRepositoryProd/BulkNew/CSV/CSV_{year}_Q{quarter}.zip"
            download_path = self.download_directory / f"ferceqr-{year}-Q{quarter}.zip"

        else:  # For 2002 - 2013 data
            part = "transaction"
            url = f"https://eqrdds.ferc.gov/eqrdbdownloads/eqr_transaction_{year}.zip"
            download_path = self.download_directory / f"ferceqr-{year}-{part}.zip"

        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "quarter": quarter, "data_type": part},
        )

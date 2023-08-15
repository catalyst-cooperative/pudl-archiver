"""Download FERC EQR data."""
import ftplib  # nosec: B402
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
    concurrency_limit = 1
    directory_per_resource_chunk = True
    max_wait_time = 36000

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC EQR resources."""
        # Get non-transaction data (pre-2013)
        # Skip all pre-2013 data until access to FTP server is figured out
        """
        for i in range(self.max_wait_time):
            self.logger.info(
                f"Waiting for EQR to be available, attempt: {i}/{self.max_wait_time}"
            )
            try:
                ftp = ftplib.FTP("eqrdds.ferc.gov")  # nosec: B321
            except Exception as e:
                self.logger.info(f"Error: {e}")
                await asyncio.sleep(1)

        yield self.get_bulk_csv(ftp)

        # Get 2002-2013 annual transaction data
        for year in range(2002, 2014):
            yield self.get_year_dbf(year, ftp)
        """

        # Get quarterly EQR data
        for year in range(2013, 2023):
            for quarter in range(1, 5):
                if quarter < 3:
                    continue
                yield self.get_quarter_dbf(year, quarter)

    async def get_bulk_csv(self, ftp: ftplib.FTP) -> tuple[Path, dict]:
        """Download all 2002-2013 non-transaction data."""
        download_path = self.download_directory / "ferceqr_nontrans.zip"
        with download_path.open() as f:
            ftp.retrbinary("RETR eqrdbdownloads/eqr_nontransaction.zip", f.write)

        return ResourceInfo(
            local_path=download_path, partitions={"data_type": "non-transaction"}
        )

    async def get_year_dbf(
        self,
        year: int,
        ftp: ftplib.FTP,
    ) -> tuple[Path, dict]:
        """Download a year (2002-2013) of FERC EQR transaction data."""
        download_path = self.download_directory / f"ferceqr-{year}-trans.zip"

        with download_path.open() as f:
            ftp.retrbinary(f"RETR eqrdbdownloads/eqr_transaction_{year}.zip", f.write)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "data_type": "transaction"},
        )

    async def get_quarter_dbf(
        self,
        year: int,
        quarter: int | None = None,
    ) -> tuple[Path, dict]:
        """Download a quarter of 2014-present data."""
        url = f"https://eqrreportviewer.ferc.gov/DownloadRepositoryProd/BulkNew/CSV/CSV_{year}_Q{quarter}.zip"
        download_path = self.download_directory / f"ferceqr-{year}-Q{quarter}.zip"

        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "quarter": quarter},
        )

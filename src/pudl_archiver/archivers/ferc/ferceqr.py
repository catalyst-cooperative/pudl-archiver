"""Download FERC EQR data."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class FercEQRArchiver(AbstractDatasetArchiver):
    """FERC EQR archiver.

    EQR data is much too large to use with Zenodo, so this archiver
    is meant to be used with be used with the `fsspec` storage
    backend. To run the archiver with this backend, execute the
    following command:

    ```
    pudl_archiver --datasets ferceqr --deposition-path gs://archives.catalyst.coop/eqr --depositor fsspec
    ```
    """

    name = "ferceqr"
    concurrency_limit = 1
    directory_per_resource_chunk = True
    max_wait_time = 36000

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC EQR resources."""
        # Get quarterly EQR data
        for year in range(2013, 2026):
            for quarter in range(1, 5):
                if (quarter < 3 and year == 2013) or (quarter > 2 and year == 2025):
                    continue
                yield self.get_quarter_csv(year, quarter)

    async def get_quarter_csv(
        self,
        year: int,
        quarter: int,
    ) -> tuple[Path, dict]:
        """Download a quarter of 2013-present data."""
        url = f"https://eqrreportviewer.ferc.gov/DownloadRepositoryProd/7D0F99CBC5C744969A7A9A5F4BA5612ED77CB30C09F6425BB9C3D417EFBE01C20C6C7A6DE7D0446881A7639F0FDC8FE1/BulkNew/CSV/CSV_{year}_Q{quarter}.zip"
        download_path = self.download_directory / f"ferceqr-{year}-Q{quarter}.zip"

        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"year": year, "quarter": quarter},
        )

"""Archiver for SEC 10-K and supporting documents."""

import gzip
import io
import logging

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")
BASE_URL = "https://www.sec.gov/Archives"


class Sec10KArchiver(AbstractDatasetArchiver):
    """SEC 10K archiver."""

    name = "sec10k"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download SEC 10-K resources."""
        for year in range(1993, 2024):
            yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> ResourceInfo:
        """Download and zip all filings for a given year."""
        year_index = self.get_year_index(year)
        form_index = year_index[year_index["Form Type"].str.startswith("10-K")]

        download_tasks = []
        for fname in form_index["Filename"]:
            url = f"{BASE_URL}/{fname}"
            buffer = io.BytesIO()
            download_tasks.append(self.download_file(url, buffer))

        year_archive = self.download_directory / f"sec10k-{year}.zip"
        async for url, buffer in self.rate_limit_tasks(download_tasks, rate_limit=10):
            fname = url.replace(f"{BASE_URL}/", "")
            self.add_to_archive(year_archive, fname, buffer)

        # Add index containing filing metadata to archive
        index_buffer = io.BytesIO()
        form_index.to_csv(index_buffer)
        self.add_to_archive(year_archive, "index.csv", index_buffer)

        return ResourceInfo(local_path=year_archive, partitions={"year": year})

    async def get_year_index(self, year: int) -> pd.DataFrame:
        """Download index file for each quarter in year and return DataFrame of index."""
        # Header is row 9 and row 10 is a delimiter
        skiprows = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10]
        qtr_indices = []
        for qtr in range(5):
            index_file = io.BytesIO()
            await self.download_file(
                f"{BASE_URL}/edgar/full-index/{year}/QTR{qtr}/master.gz",
                index_file,
            )

            with gzip.open(index_file.getvalue()) as f:
                df = pd.read_csv(f, sep="|", skiprows=skiprows)
                df["quarter"] = qtr
                qtr_indices.append(df)

        return pd.concat(qtr_indices)

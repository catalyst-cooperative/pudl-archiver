"""Archiver for SEC 10-K and supporting documents."""

import asyncio
import gzip
import io
import logging

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.utils import rate_limit_tasks

logger = logging.getLogger(f"catalystcoop.{__name__}")
BASE_URL = "https://www.sec.gov/Archives"
USER_AGENT_HEADER = {"User-Agent": "Catalyst Cooperative pudl@catalyst.coop"}


class Sec10KArchiver(AbstractDatasetArchiver):
    """SEC 10K archiver."""

    name = "sec10k"
    concurrency_limit = 1
    resumeable = True

    async def get_resources(
        self,
    ) -> ArchiveAwaitable:
        """Download SEC 10-K resources."""
        for year in range(1993, 2024):
            if any([str(year) in fname for fname in self.existing_files]):  # noqa: C419
                logger.info(f"Skipping {year}, archive already exists in deposition.")
            else:
                yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> ResourceInfo:
        """Download and zip all filings for a given year."""
        logger.info(f"Downloading files from {year}")

        # Wait one second before starting next year to clear rate limit
        await asyncio.sleep(1)

        year_index = await self.get_year_index(year)
        form_index = year_index[year_index["Form Type"].str.startswith("10-K")]

        # Wait one second before starting next year to clear rate limit
        await asyncio.sleep(1)

        download_tasks = []
        for fname in form_index["Filename"]:
            url = f"{BASE_URL}/{fname}"
            buffer = io.BytesIO()
            download_tasks.append(
                self.download_file(url, buffer, headers=USER_AGENT_HEADER)
            )

        year_archive = self.download_directory / f"sec10k-{year}.zip"
        async for url, buffer in rate_limit_tasks(download_tasks, rate_limit=8):
            logger.info(f"Downloaded: {url}")
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
        for qtr in range(1, 5):
            logger.info(f"Getting index for q{qtr}")
            index_file = self.download_directory / "index.gz"
            await self.download_file(
                f"{BASE_URL}/edgar/full-index/{year}/QTR{qtr}/master.gz",
                index_file,
                headers=USER_AGENT_HEADER,
            )

            with gzip.open(index_file, mode="rt") as f:
                try:
                    df = pd.read_csv(f, sep="|", skiprows=skiprows)
                except UnicodeDecodeError:
                    df = pd.read_csv(f, sep="|", skiprows=skiprows, encoding="latin-1")
                df["quarter"] = qtr
                qtr_indices.append(df)

        return pd.concat(qtr_indices)

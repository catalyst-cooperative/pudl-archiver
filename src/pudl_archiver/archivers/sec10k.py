"""Archiver for SEC 10-K and supporting documents."""

import asyncio
import gzip
import io
import logging
import re

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.depositors.gcs import GCSDepositor
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import rate_limit_tasks

logger = logging.getLogger(f"catalystcoop.{__name__}")
BASE_URL = "https://www.sec.gov/Archives"
USER_AGENT_HEADER = {"User-Agent": "Catalyst Cooperative pudl@catalyst.coop"}


class Sec10KArchiver(AbstractDatasetArchiver):
    """SEC 10K archiver."""

    name = "sec10k"
    concurrency_limit = 1
    resumeable = True
    fail_on_file_size_change = False
    fail_on_dataset_size_change = False
    directory_per_resource_chunk = True
    ex_21_pattern = re.compile(r"<DOCUMENT>\s?<TYPE>EX-(21(\.\d)?)")
    depositor = GCSDepositor

    async def get_resources(
        self,
    ) -> ArchiveAwaitable:
        """Download SEC 10-K resources."""
        for year in range(1993, 2024):
            for quarter in range(1, 5):
                if year > 1993 or quarter > 3:
                    yield self.get_year_quarter(year, quarter)

    async def get_year_quarter(self, year: int, quarter: int) -> ResourceInfo:
        """Download and zip all filings for a given year."""
        logger.info(f"Downloading files from {year}q{quarter}")

        # Wait one second before starting next year to clear rate limit
        await asyncio.sleep(1)

        quarter_index = await self.get_quarter_index(year, quarter)
        quarter_index["year_quarter"] = f"{year}q{quarter}"
        form_index = quarter_index[
            quarter_index["Form Type"].str.startswith("10-K")
        ].copy()
        form_index["exhibit_21_version"] = None

        # Wait one second before starting next year to clear rate limit
        await asyncio.sleep(1)

        download_tasks = []
        for fname in form_index["Filename"]:
            url = f"{BASE_URL}/{fname}"
            buffer = io.BytesIO()
            download_tasks.append(
                self.download_file(url, buffer, headers=USER_AGENT_HEADER)
            )

        year_archive = self.download_directory / f"sec10k-{year}q{quarter}.zip"
        zip_files = set()
        async for url, buffer in rate_limit_tasks(download_tasks, rate_limit=8):
            logger.info(f"Downloaded: {url}")
            fname = url.replace(f"{BASE_URL}/", "")
            # Check for exhibit 21
            try:
                has_ex_21 = self.ex_21_pattern.search(buffer.getvalue().decode())
            except UnicodeDecodeError:
                # Some files have inconsistent encoding. Change to utf-8 before archiving
                file_text = buffer.getvalue().decode(encoding="latin-1")
                buffer = io.BytesIO(initial_bytes=file_text.encode())
                has_ex_21 = self.ex_21_pattern.search(file_text)
            if has_ex_21:
                form_index.loc[
                    form_index["Filename"] == fname, ["exhibit_21_version"]
                ] = has_ex_21.group(1)

            self.add_to_archive(year_archive, fname, buffer)
            zip_files.add(fname)

        # Add index containing filing metadata to archive
        # index_buffer = io.BytesIO()
        # form_index.to_csv(index_buffer, index=False)
        # self.add_to_archive(year_archive, "index.csv", index_buffer)

        logger.info(f"Finished downloading filings from {year}q{quarter}.")
        return ResourceInfo(
            local_path=year_archive,
            partitions={"year_quarter": f"{year}q{quarter}"},
            layout=ZipLayout(file_paths=zip_files),
            metadata=form_index,
        )

    async def get_quarter_index(self, year: int, quarter: int) -> pd.DataFrame:
        """Download index file for each quarter in year and return DataFrame of index."""
        # Header is row 9 and row 10 is a delimiter
        skiprows = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10]
        index_file = self.download_directory / "index.gz"
        await self.download_file(
            f"{BASE_URL}/edgar/full-index/{year}/QTR{quarter}/master.gz",
            index_file,
            headers=USER_AGENT_HEADER,
        )

        try:
            with gzip.open(index_file, mode="rt") as f:
                df = pd.read_csv(f, sep="|", skiprows=skiprows)
        except UnicodeDecodeError:
            with gzip.open(index_file, mode="rt", encoding="latin-1") as f:
                df = pd.read_csv(f, sep="|", skiprows=skiprows)

        return df

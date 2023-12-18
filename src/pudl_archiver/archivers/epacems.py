"""Download EPACEMS data."""
import datetime
import json
import logging
import os
import zipfile
from collections.abc import Iterable
from itertools import groupby

import requests
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class BulkFile(BaseModel):
    """Data transfer object from EPA.

    See https://www.epa.gov/power-sector/cam-api-portal#/swagger/camd-services
    for details.
    """

    class Metadata(BaseModel):
        """Metadata about a specific file."""

        year: int | None = None
        quarter: int | None = None
        data_type: str
        data_sub_type: str | None = None

        class Config:  # noqa: D106
            alias_generator = to_camel
            allow_population_by_field_name = True

    filename: str
    s3_path: str
    bytes: int  # noqa: A003
    mega_bytes: float
    giga_bytes: float
    last_updated: datetime.datetime
    metadata: Metadata

    class Config:  # noqa: D106
        alias_generator = to_camel
        allow_population_by_field_name = True


class EpaCemsArchiver(AbstractDatasetArchiver):
    """EPA CEMS archiver."""

    name = "epacems"
    concurrency_limit = 2  # Number of files to concurrently download

    base_url = "https://api.epa.gov/easey/bulk-files/"
    parameters = {"api_key": os.environ["EPACEMS_API_KEY"]}  # Set to API key

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA CEMS resources."""
        file_list = requests.get(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=self.parameters,
            timeout=300,
        )
        if file_list.status_code == 200:
            resjson = file_list.content.decode("utf8").replace("'", '"')
            file_list.close()  # Close connection.
            bulk_files = (BulkFile(**f) for f in json.loads(resjson))
            quarterly_emissions_files = [
                file
                for file in bulk_files
                if (file.metadata.data_type == "Emissions")
                and (file.metadata.data_sub_type == "Hourly")
                and (file.metadata.quarter in {1, 2, 3, 4})
                and self.valid_year(file.metadata.year)
            ]
            logger.info(f"Downloading {len(quarterly_emissions_files)} total files.")
            logger.debug(f"File info: {quarterly_emissions_files}")
            files_by_year = groupby(
                sorted(quarterly_emissions_files, key=lambda bf: bf.metadata.year or 0),
                lambda bf: bf.metadata.year,
            )
            for year, files in files_by_year:
                yield self.get_year_resource(year, list(files))
        else:
            raise AssertionError(
                f"EPACEMS API request did not succeed: {file_list.status_code}"
            )

    async def get_year_resource(
        self, year: int, files: Iterable[BulkFile]
    ) -> ResourceInfo:
        """Download all available data for a single quarter in a year.

        Args:
            file: a dictionary containing file characteristics from the EPA API.
                See https://www.epa.gov/power-sector/cam-api-portal#/swagger/camd-services
                for expected format of dictionary.
            request_count: the number of the request.
        """
        archive_path = self.download_directory / f"epacems-{year}.zip"
        for file in files:
            url = self.base_url + file.s3_path
            quarter = file.metadata.quarter

            # Useful to debug at download time-outs.
            logger.info(f"Downloading {year} Q{quarter} EPACEMS data from {url}.")

            # Create zipfile to store year/quarter combinations of files
            filename = f"epacems-{year}q{quarter}.csv"
            file_path = self.download_directory / filename

            await self.download_file(url=url, file=file_path, timeout=60 * 14)

            with zipfile.ZipFile(
                archive_path,
                "a",
                compression=zipfile.ZIP_DEFLATED,
            ) as archive, file_path.open("rb") as f:
                archive.writestr(filename, f.read())

            file_path.unlink()

        return ResourceInfo(
            local_path=archive_path,
            partitions={
                "year": year,
                "quarter": sorted([file.metadata.quarter for file in files]),
            },
        )

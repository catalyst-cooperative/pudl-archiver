"""Download EPAMATS data."""

import json
import logging
import os
from collections.abc import Iterable
from itertools import groupby

import requests
from pydantic import ValidationError

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.epa.epacems import BulkFile
from pudl_archiver.frictionless import ZipLayout

logger = logging.getLogger(f"catalystcoop.{__name__}")


class EpaMatsArchiver(AbstractDatasetArchiver):
    """EPA MATS archiver."""

    name = "epamats"
    allowed_file_rel_diff = 0.35  # Set higher tolerance than standard

    base_url = "https://api.epa.gov/easey/bulk-files/"
    # Set API key to CEMS key - CEMS and MATS come from the same API
    parameters = {"api_key": os.environ.get("EPACEMS_API_KEY")}

    def __filter_for_complete_metadata(
        self, files_responses: list[dict]
    ) -> Iterable[BulkFile]:
        """Silently drop files that don't have year/data-subtype/etc."""
        for f in files_responses:
            try:
                yield BulkFile(**f)
            except ValidationError:
                continue

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA MATS resources."""
        file_list = requests.get(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=self.parameters,
            timeout=300,
        )
        if file_list.status_code != 200:
            raise AssertionError(
                f"EPA MATS API request did not succeed: {file_list.status_code}"
            )
        resjson = file_list.content.decode("utf8").replace("'", '"')
        file_list.close()  # Close connection.
        bulk_files = self.__filter_for_complete_metadata(json.loads(resjson))
        quarterly_emissions_files = [
            file
            for file in bulk_files
            if (file.metadata.data_type == "Mercury and Air Toxics Emissions (MATS)")
            and (file.metadata.data_sub_type == "Hourly")
            and (file.metadata.quarter in {1, 2, 3, 4})
            and self.valid_year(file.metadata.year)
        ]
        logger.info(f"Downloading {len(quarterly_emissions_files)} total files.")
        logger.debug(f"File info: {quarterly_emissions_files}")
        files_by_year = groupby(
            sorted(quarterly_emissions_files, key=lambda bf: bf.metadata.year),
            lambda bf: bf.metadata.year,
        )
        for year, files in files_by_year:
            yield self.get_year_resource(year, list(files))

    async def get_year_resource(
        self, year: int, files: Iterable[BulkFile]
    ) -> ResourceInfo:
        """Download all available data for a year.

        Args:
            year: the year we're downloading data for
            files: the files we've associated with this year.
        """
        zip_path = self.download_directory / f"epamats-{year}.zip"
        data_paths_in_archive = set()
        for file in files:
            url = self.base_url + file.s3_path
            quarter = file.metadata.quarter

            # Useful to debug at download time-outs.
            logger.info(f"Downloading {year} Q{quarter} EPA MATS data from {url}.")

            filename = f"epamats-{year}q{quarter}.csv"
            file_path = self.download_directory / filename
            await self.download_file(url=url, file_path=file_path)
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=file_path.open("rb"),
            )
            data_paths_in_archive.add(filename)
            # Don't want to leave multiple giant CSVs on disk, so delete
            # immediately after they're safely stored in the ZIP
            file_path.unlink()

        return ResourceInfo(
            local_path=zip_path,
            partitions={
                "year_quarter": sorted(
                    [f"{year}q{file.metadata.quarter}" for file in files]
                ),
            },
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

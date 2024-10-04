"""Archive VCE renewable generation data.

This dataset was produced by Vibrant Clean Energy, and is licensed to the public under
the Creative Commons Attribution 4.0 International license (CC-BY-4.0). It is archived
from files stored in the private sources.catalyst.coop bucket.
"""

import logging
import re
from pathlib import Path

from google.cloud import storage

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class VCEReGenArchiver(AbstractDatasetArchiver):
    """VCE Renewable Generation Profiles archiver."""

    name = "vceregen"
    bucket_name = "sources.catalyst.coop"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download VCE renewable generation resources."""
        bucket = storage.Client().get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=f"{self.name}/")  # Get all blobs in folder

        for blob in blobs:
            # Skip the folder, which appears in this list
            if not blob.name.endswith("/"):
                yield self.get_gcs_resource(blob)

    async def get_gcs_resource(self, blob: storage.Blob) -> tuple[Path, dict]:
        """Download VCE renewable generation profile files from GCS.

        There are three types of files: a documentation PDF, a single CSV and a series
        of zipped annual files that are named vceregen_{year}.zip.
        """
        # Remove folder name (identical to dataset name) and set download path
        file_name = blob.name.replace(f"{self.name}/", "")
        path_to_file = self.download_directory / file_name
        # Download blob to local file
        logger.info(f"Downloading {blob.name} to {path_to_file}")
        blob.download_to_filename(path_to_file)

        # Set up partitions:
        # vceregen-ra-county-lat-long-fips.csv should have partition fips: true
        # vceregen-YYYY.zip should have partition year: YYYY
        # The documentation file should have no partitions, as we don't ETL it.

        # Handle annual zip files
        annual_zip_pattern = re.compile(
            rf"^{self.name}-(\d{{4}}).zip"
        )  # Double escape the {4}
        annual_match = annual_zip_pattern.match(file_name)
        if annual_match:
            year = int(annual_match.group(1))
            return ResourceInfo(
                local_path=path_to_file,
                partitions={"year": year},
            )

        # Handle single lat/lon/FIPS CSV
        if file_name == "vceregen-ra-county-lat-long-fips.csv":
            return ResourceInfo(
                local_path=path_to_file,
                partitions={"fips": True},
            )

        # Handle documentation
        if file_name.endswith(".pdf"):
            return ResourceInfo(local_path=path_to_file, partitions={})

        raise AssertionError(
            f"New file {file_name} detected. Update the archiver to process it."
        )

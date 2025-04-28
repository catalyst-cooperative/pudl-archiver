"""Archive VCE Resource Adequacy Renewable Energy (RARE) data.

This dataset was produced by Vibrant Clean Energy, and is licensed to the public under
the Creative Commons Attribution 4.0 International license (CC-BY-4.0). It is archived
from files stored in the private sources.catalyst.coop bucket.
"""

import re
from pathlib import Path

from google.cloud import storage

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class VCERAREArchiver(AbstractDatasetArchiver):
    """VCE RARE data archiver."""

    name = "vcerare"
    bucket_name = "sources.catalyst.coop"
    version = "v2"  # Version of the files to archive

    async def get_resources(self) -> ArchiveAwaitable:
        """Download VCE RARE resources."""
        bucket = storage.Client().get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(
            prefix=f"{self.name}/{self.version}"
        )  # Get all blobs in folder

        for blob in blobs:
            # Skip the folder, which appears in this list
            if not blob.name.endswith("/"):
                yield self.get_gcs_resource(blob)

    async def get_gcs_resource(self, blob: storage.Blob) -> tuple[Path, dict]:
        """Download VCE renewable generation profile files from GCS.

        There are three types of files: a documentation PDF, a single CSV and a series
        of zipped annual files that are named vcerare_{year}.zip.
        """
        # Remove folder name (identical to dataset name) and set download path
        file_name = blob.name.replace(f"{self.name}/{self.version}/", "")
        path_to_file = self.download_directory / file_name
        # Download blob to local file
        self.logger.info(f"Downloading {blob.name} to {path_to_file}")
        blob.download_to_filename(path_to_file)

        # Set up partitions:
        # vcerare-county-lat-long-fips.csv should have partition fips: true
        # vcerare-YYYY.zip should have partition year: YYYY
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
        if file_name == "vcerare-county-lat-long-fips.csv":
            return ResourceInfo(
                local_path=path_to_file,
                partitions={"fips": True},
            )

        # Handle documentation and README
        if file_name.endswith(".pdf") or file_name.endswith(".md"):
            return ResourceInfo(local_path=path_to_file, partitions={})

        raise AssertionError(
            f"New file {file_name} detected. Update the archiver to process it."
        )

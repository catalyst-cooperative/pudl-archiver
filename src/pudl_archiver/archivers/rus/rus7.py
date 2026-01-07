"""Archive USDA Rural Utility Service Form 7 data.

This dataset was obtained through Freedom of Information Act (FOIA) requests. It is
archived from files stored in the private sources.catalyst.coop GCS bucket.
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

    name = "rus7"
    bucket_name = "sources.catalyst.coop"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download VCE RARE resources."""
        bucket = storage.Client().get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=f"{self.name}")  # Get all blobs in folder

        for blob in blobs:
            # Skip the folder, which appears in this list
            if not blob.name.endswith("/"):
                yield self.get_gcs_resource(blob)

    async def get_gcs_resource(self, blob: storage.Blob) -> tuple[Path, dict]:
        """Download RUS Form 7 files from GCS.

        There is one type of files: a series of zipped annual files that are named
        rus7_{year}.zip.
        """
        # Remove folder name (identical to dataset name) and set download path
        file_name = blob.name.replace(f"{self.name}/", "")
        path_to_file = self.download_directory / file_name
        # Download blob to local file
        self.logger.info(f"Downloading {blob.name} to {path_to_file}")
        blob.download_to_filename(path_to_file)

        # Set up partitions:
        # rus-YYYY.zip should have partition year: YYYY

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

        raise AssertionError(
            f"New file {file_name} detected. Update the archiver to process it."
        )

"""Archive GridPath RA toolkit renewable generation data.

This dataset was produced by Moment Energy Insights (now Sylvan Energy Analytics).
It is archived from files stored in the private sources.catalyst.coop bucket.
"""

import logging
from pathlib import Path

from google.cloud import storage

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class GridPathRAToolkitArchiver(AbstractDatasetArchiver):
    """GridPath RA Toolkit renewable generation profiles archiver."""

    name = "gridpathratoolkit"
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

        # The partition should be the filename without the filetype extension.
        # E.g., solar_capacity_aggregations.csv has part: solar_capacity_aggregations

        part = file_name.split(".")[0]  # Remove .csv/.zip extension
        return ResourceInfo(
            local_path=path_to_file,
            partitions={"part": part},
        )

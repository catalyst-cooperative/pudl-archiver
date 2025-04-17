"""Download all bulk data files from EIA API."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EiaBulkFileArchiver(AbstractDatasetArchiver):
    """EIA bulk file archiver."""

    name = "eia_bulk"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download all EIA bulk API resources."""
        # Query manifest file to get all existing bulk datasets
        bulk_manifest = await self.get_json(
            "https://www.eia.gov/opendata/bulk/manifest.txt"
        )
        for dataset in bulk_manifest["dataset"]:
            # Assign dataset name as partition and file name
            dataset_name = bulk_manifest["dataset"][dataset]["title"].lower()
            dataset_name = re.sub("[^0-9a-zA-Z ]+", "", dataset_name).replace(" ", "-")
            dataset_url = bulk_manifest["dataset"][dataset]["accessURL"]
            yield self.get_bulk_resource(dataset_name=dataset_name, url=dataset_url)

    async def get_bulk_resource(self, dataset_name=str, url=str) -> tuple[Path, dict]:
        """Download bulk zip file."""
        download_path = self.download_directory / f"eia-bulk-{dataset_name}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"data_set": dataset_name}
        )

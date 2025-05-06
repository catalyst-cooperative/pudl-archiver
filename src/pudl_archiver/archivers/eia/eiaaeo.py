"""Download EIA Annual Energy Outlook data."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.eia.gov/opendata/bulk"


class EiaAeoArchiver(AbstractDatasetArchiver):
    """EIA AEO archiver."""

    name = "eiaaeo"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA AEO resources."""
        # Use the bulk manifest to get the years of available AEO data
        bulk_manifest = await self.get_json(
            "https://www.eia.gov/opendata/bulk/manifest.txt"
        )
        aeo_datasets = [
            dataset for dataset in bulk_manifest["dataset"] if "AEO" in dataset
        ]
        year_pattern = re.compile(r"\d{4}$")
        # Sort, but use a set to avoid accidental duplication of years
        aeo_available_years = sorted(
            {
                int(year_pattern.search(dataset).group())
                for dataset in aeo_datasets
                if year_pattern.search(dataset) is not None
            }
        )

        for year in aeo_available_years:
            if self.valid_year(year):
                # Get URL from manifest file
                url = bulk_manifest["dataset"][f"AEO.{year}"]["accessURL"]
                yield self.get_year_resource(year=year, url=url)

    async def get_year_resource(self, year: int, url: str) -> tuple[Path, dict]:
        """Download zip file."""
        download_path = self.download_directory / f"eiaaeo-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})

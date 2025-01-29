"""Archive EIA  Commercial Buildings Energy Consumption Survey (CBECS)."""

import logging
import re
from pathlib import Path
from urllib.parse import urljoin

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/consumption/commercial/data/"
logger = logging.getLogger(f"catalystcoop.{__name__}")


class EiaCbecsArchiver(AbstractDatasetArchiver):
    """EIA CBECS archiver."""

    name = "eiacbecs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-CBECS resources."""
        link_pattern = re.compile(r"commercial/data/(\d{4})/$", re.IGNORECASE)

        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            match = link_pattern.search(link)
            year = match.group(1)
            yield self.get_year_resources(year)

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all excel tables for a year."""
        data_paths_in_archive = set()
        zip_path = self.download_directory / f"eiacbecs-{year}.zip"
        pattern = rf"{year}(?:.*)/([a-z,\d]{{1,5}})(.xls|.xlsx|.pdf)$"
        data_view_patterns = {
            "characteristics": re.compile(pattern),
            "consumption": re.compile(pattern),
            "mircodata": re.compile(
                rf"{year}/(?:xls|pdf|csv)/(.*)(.xls|.xlsx|.pdf|.csv)$"
            ),
        }
        for view, table_link_pattern in data_view_patterns.items():
            year_url = f"{BASE_URL}{year}/index.php?view={view}"
            for link in await self.get_hyperlinks(year_url, table_link_pattern):
                match = table_link_pattern.search(link)
                unique_id = match.group(1)
                file_extension = match.group(2)
                filename = f"eiacbecs-{year}-{view}-{unique_id}{file_extension}"
                file_url = urljoin(year_url, link)
                download_path = self.download_directory / filename
                await self.download_file(file_url, download_path)
                with Path.open(download_path, "rb") as f:
                    first_bytpes = f.read(20)
                    if b"html" in first_bytpes.lower().strip():
                        logger.warning(
                            f"Skipping {file_url} because it appears to be a redirect/html page."
                        )
                        pass
                    else:
                        self.add_to_archive(
                            zip_path=zip_path,
                            filename=filename,
                            blob=download_path.open("rb"),
                        )
                        data_paths_in_archive.add(filename)
                        # Don't want to leave multiple giant CSVs on disk, so delete
                        # immediately after they're safely stored in the ZIP
                        download_path.unlink()

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

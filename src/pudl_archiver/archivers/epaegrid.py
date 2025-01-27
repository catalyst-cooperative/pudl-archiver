"""Download EPA eGRID data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.epa.gov/egrid/historical-egrid-data"


class EpaEgridArchiver(AbstractDatasetArchiver):
    """EPA eGrid archiver."""

    name = "epaegrid"
    # concurrency_limit = 1  # Number of files to concurrently download

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EPA eGrid resources."""
        # All of the "historical" data is stored on one page while the most
        # recent data is stored on the main dataset page. So we need to
        # go grab all the old data first and then get the newest data.
        link_pattern = re.compile(r"egrid(\d{4})_data(_v(\d{1})|).xlsx", re.IGNORECASE)
        years = []
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            match = link_pattern.search(link)
            year = int(match.group(1))
            years = years + [year]
            yield self.get_year_resource(year, [BASE_URL], False)

        recent_year = max(years) + 1
        recent_urls = [
            "https://www.epa.gov/egrid/detailed-data",
            "https://www.epa.gov/egrid/summary-data",
            "https://www.epa.gov/egrid/egrid-technical-guide",
        ]
        yield self.get_year_resource(recent_year, recent_urls, True)

        # TODO: add the PM files as their own separate zip
        # https://www.epa.gov/egrid/egrid-pm25

    async def get_year_resource(
        self, year: int, base_urls: list[str], is_recent: bool
    ) -> ResourceInfo:
        """Download xlsx file."""
        zip_path = self.download_directory / f"epaegrid-{year}.zip"
        table_link_pattern = re.compile(
            rf"egrid{year}_([a-z,_\d]*)(.xlsx|.pdf|.txt)", re.IGNORECASE
        )
        data_paths_in_archive = set()
        for base_url in base_urls:
            for link in await self.get_hyperlinks(base_url, table_link_pattern):
                match = table_link_pattern.search(link)
                table = match.group(1)
                file_extension = match.group(2)
                filename = f"epaegrid-{year}-{table}{file_extension}"
                download_path = self.download_directory / filename
                await self.download_file(link, download_path)
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

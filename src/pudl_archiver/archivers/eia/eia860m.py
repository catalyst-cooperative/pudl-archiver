"""Download EIA-860M data."""

import calendar
import re
from collections import defaultdict

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/electricity/data/eia860m"


class Eia860MArchiver(AbstractDatasetArchiver):
    """EIA-860M archiver."""

    name = "eia860m"
    month_map = {
        name.lower(): number for number, name in enumerate(calendar.month_name)
    }

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-860M resources."""
        link_pattern = re.compile(r"([a-z]+)_generator(\d{4}).xlsx")

        year_links: dict[int, dict[int, str]] = defaultdict(dict)
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            match = link_pattern.search(link)
            if not match:
                continue
            year = int(match.group(2))
            month = self.month_map[match.group(1)]
            if self.valid_year(year):
                year_links[year][month] = link

        for year, month_links in year_links.items():
            yield self.get_year_resource(year, month_links)

    async def get_year_resource(
        self, year: int, month_links: dict[int, str]
    ) -> ResourceInfo:
        """Download xlsx file."""
        zip_path = self.download_directory / f"eia860m-{year}.zip"
        data_paths_in_archive = set()
        for month, link in sorted(month_links.items()):
            url = f"https://eia.gov/{link}"
            filename = f"eia860m-{year}-{month:02}.xlsx"
            download_path = self.download_directory / filename
            await self.download_file(url, download_path)
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
            partitions={
                "year_month": sorted([f"{year}-{month:02}" for month in month_links])
            },
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

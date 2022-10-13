"""Defines base class for archiver."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable


class Ferc6Archiver(AbstractDatasetArchiver):
    name = "ferc6"

    def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 6 resources."""
        for year in range(2000, 2022):
            yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 6 data."""
        url = f"https://forms.ferc.gov/f6allyears/f6_{year}.zip"
        download_path = self.download_directory / f"ferc6-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}

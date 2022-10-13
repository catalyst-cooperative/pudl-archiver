"""Defines base class for archiver."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable


class Ferc1Archiver(AbstractDatasetArchiver):
    name = "ferc1"

    def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 1 resources."""
        for year in range(1994, 1995):
            yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 1 data."""
        url = f"https://forms.ferc.gov/f1allyears/f1_{year}.zip"
        download_path = self.download_directory / f"ferc1-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}

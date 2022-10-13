"""Defines base class for archiver."""
from pathlib import Path

from pudl_scrapers.archiver.classes import AbstractDatasetArchiver, ArchiveAwaitable


class Ferc60Archiver(AbstractDatasetArchiver):
    name = "ferc60"

    def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 60 resources."""
        for year in range(2006, 2021):
            yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 60 data."""
        url = f"https://forms.ferc.gov/f60allyears/f60_{year}.zip"
        download_path = self.download_directory / f"ferc60-{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}

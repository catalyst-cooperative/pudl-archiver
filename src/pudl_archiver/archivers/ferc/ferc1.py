"""Download FERC Form 1 data."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.ferc import xbrl


class Ferc1Archiver(AbstractDatasetArchiver):
    """Ferc Form 1 archiver."""

    name = "ferc1"
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC 1 resources."""
        for year in range(1994, 2022):
            if self.valid_year(year):
                yield self.get_year_dbf(year)

        yield xbrl.archive_xbrl_for_form(
            xbrl.FercForm.FORM_1, self.download_directory, self.valid_year, self.session
        )

    async def get_year_dbf(self, year: int) -> tuple[Path, dict]:
        """Download a single year of FERC Form 1 data."""
        url = f"https://forms.ferc.gov/f1allyears/f1_{year}.zip"
        download_path = self.download_directory / f"ferc1-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(
            local_path=download_path, partitions={"year": year, "data_format": "DBF"}
        )

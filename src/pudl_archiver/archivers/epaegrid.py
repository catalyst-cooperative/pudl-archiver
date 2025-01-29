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
            years += [year]
            if self.valid_year(year):
                yield self.get_year_resource(
                    year, [BASE_URL, "https://www.epa.gov/egrid/egrid-pm25"]
                )

        recent_year = max(years) + 1
        recent_urls = [
            "https://www.epa.gov/egrid/detailed-data",
            "https://www.epa.gov/egrid/summary-data",
            "https://www.epa.gov/egrid/egrid-technical-guide",
            "https://www.epa.gov/egrid/egrid-pm25",
        ]
        if self.valid_year(recent_year):
            yield self.get_year_resource(recent_year, recent_urls)

    async def _download_add_unlink(self, link: str, filename: str, zip_path: str):
        """Download the file, add it to an zip file in the archive and unlink.

        Little helper function because we are doing this same pattern several times
        for this dataset within :meth`get_year_resource` because the data is stored
        across several pages or have bespoke patterns.
        """
        download_path = self.download_directory / filename
        await self.download_file(link, download_path)
        self.add_to_archive(
            zip_path=zip_path,
            filename=filename,
            blob=download_path.open("rb"),
        )
        # Don't want to leave multiple files on disk, so delete
        # immediately after they're safely stored in the ZIP
        download_path.unlink()

    async def get_year_resource(self, year: int, base_urls: list[str]) -> ResourceInfo:
        """Download all files pertaining to an eGRID year."""
        zip_path = self.download_directory / f"epaegrid-{year}.zip"
        table_link_pattern = re.compile(
            rf"egrid{year}(?:_|-)([a-z,_\d,-]*)(.xlsx|.pdf|.txt)$", re.IGNORECASE
        )
        data_paths_in_archive = set()
        for base_url in base_urls:
            for link in await self.get_hyperlinks(base_url, table_link_pattern):
                match = table_link_pattern.search(link)
                # TODO: this setup leaves in all the _rev# _r# _r#_# and _{date}
                # in this table name. It would be ideal to remove this all together
                table = match.group(1).replace("_", "-").lower().strip()
                file_extension = match.group(2)
                filename = f"epaegrid-{year}-{table}{file_extension}"
                await self._download_add_unlink(link, filename, zip_path)
                data_paths_in_archive.add(filename)
        # there is one file with PM 2.5 data in it which says its for 2018-2022
        # add this file to every one of the yearly zips
        pm_combo_years = [2018, 2019, 2020, 2021]
        if year in pm_combo_years:
            link = "https://www.epa.gov/system/files/documents/2024-06/egrid-draft-pm-emissions.xlsx"
            filename = f"epaegrid-{year}-pm-emissions.xlsx"
            await self._download_add_unlink(link, filename, zip_path)
            data_paths_in_archive.add(filename)
        # There are two special case links on the PM 2.5 page that don't adhere to a
        # clear pattern. so we'll hardcode how to grab them.
        pm_special_year_links = {
            2020: "https://www.epa.gov/system/files/documents/2022-12/eGRID2020%20DRAFT%20PM%20Memo.pdf",
            2019: "https://www.epa.gov/system/files/documents/2023-01/DRAFT%202019%20PM%20Memo.pdf",
            2018: "https://www.epa.gov/sites/default/files/2020-07/documents/draft_egrid_pm_white_paper_7-20-20.pdf",
        }
        if year in pm_special_year_links:
            link = pm_special_year_links[year]
            filename = f"epaegrid-{year}-pm-emissions-methodology.pdf"
            await self._download_add_unlink(link, filename, zip_path)
            data_paths_in_archive.add(filename)
        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

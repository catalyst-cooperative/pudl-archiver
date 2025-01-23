"""Download DOE LEAD data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool"

# verified working 2025-01-22 via
# $ wget "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool" -O foo.html -U "Mozilla/5.0 Catalyst/2025 Cooperative/2025"
HEADERS = {"User-Agent": "Mozilla/5.0 Catalyst/2025 Cooperative/2025"}


class DoeLeadArchiver(AbstractDatasetArchiver):
    """DOE LEAD archiver."""

    name = "doelead"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download DOE LEAD resources."""
        # e.g.: https://data.openei.org/submissions/6219
        oei_link_pattern = re.compile(r"data\.openei\.org/submissions")
        # e.g.: https://data.openei.org/files/6219/DC-2022-LEAD-data.zip
        #       https://data.openei.org/files/6219/Data%20Dictionary%202022.xlsx
        #       https://data.openei.org/files/6219/LEAD%20Tool%20States%20List%202022.xlsx
        data_link_pattern = re.compile(r"([^/]+(\d{4})(?:-LEAD-data.zip|.xlsx))")
        for oei_link in await self.get_hyperlinks(
            BASE_URL, oei_link_pattern, headers=HEADERS
        ):
            self.logger.info(f"LEAD tool raw dataset: {oei_link}")
            year_links = {}
            oei_year = -1
            for data_link in await self.get_hyperlinks(oei_link, data_link_pattern):
                matches = data_link_pattern.search(data_link)
                if not matches:
                    continue
                link_year = int(matches.group(2))
                if oei_year < 0:
                    oei_year = link_year
                else:
                    if oei_year != link_year:
                        self.logger.warning(
                            f"Mixed years found at {oei_link}: {oei_year}, {link_year} from {data_link}"
                        )
                self.logger.debug(f"OEI data: {data_link}")
                year_links[matches.group(1)] = data_link
            if year_links:
                self.logger.info(f"Downloading: {oei_year}, {len(year_links)} items")
                yield self.get_year_resource(year_links, oei_year)
        self.logger.info("ALL DONE")

    async def get_year_resource(self, links: dict[str, str], year: int) -> ResourceInfo:
        """Download zip file."""
        host = "https://data.openei.org"
        zip_path = self.download_directory / f"doelead-{year}.zip"
        data_paths_in_archive = set()
        for filename, link in sorted(links.items()):
            self.logger.info(f"Downloading {link}")
            download_path = self.download_directory / filename
            await self.download_file(f"{host}{link}", download_path)
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=download_path.open("rb"),
            )
            data_paths_in_archive.add(filename)
            # Don't want to leave multiple giant files on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()
        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

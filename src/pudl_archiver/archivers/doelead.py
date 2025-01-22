"""Download DOE LEAD data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool"

# verified working 2025-01-22 via
# $ wget "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool" -O foo.html -U "Mozilla/5.0 Catalyst/2025 Cooperative/2025"
HEADERS = {"User-Agent":"Mozilla/5.0 Catalyst/2025 Cooperative/2025"}

class DoeLeadArchiver(AbstractDatasetArchiver):
    """DOE LEAD archiver."""

    name = "doelead"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download DOE LEAD resources."""
        # https://data.openei.org/submissions/6219
        link_pattern = re.compile(r"data.openei.org")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern, headers=HEADERS):
            matches = link_pattern.search(link)
            if not matches:
                continue
            self.logger.info(f"LINK: {link}")
            if False:
            	yield self.get_year_resource()
        self.logger.info("ALL DONE")
#        yield self.get_year_resource()
#             year = int(matches.group(1))
#             if self.valid_year(year):
#                 yield self.get_year_resource(link, year)

    async def get_year_resource(self) -> ResourceInfo:
        """Download zip file."""
        # Append hyperlink to base URL to get URL of file
        return ResourceInfo(local_path=self.download_directory / "foo", partitions={})
#         url = f"{BASE_URL}/{link}"
#         download_path = self.download_directory / f"eia860-{year}.zip"
#         await self.download_zipfile(url, download_path)
# 
#         return ResourceInfo(local_path=download_path, partitions={"year": year})

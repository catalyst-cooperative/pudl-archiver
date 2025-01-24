"""Download DOE LEAD data.

Each partition includes:
- Data Dictionary
- Census Tracts List
- Cities List
- Counties List
- States List
- Tribal Areas List
- Cities Census Track Overlaps
- Tribal Areas Tract Overlaps
- One .zip file per state, each of which includes:
  - AMI Census Tracts
  - SMI Census Tracts
  - LLSI Census Tracts
  - FPL Census Tracts
  - LLSI Counties
  - SMI Counties
  - FPL Counties
  - AMI Counties
"""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

TOOL_URL = "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool"
YEARS_DOIS = {
    2022: "https://doi.org/10.25984/2504170",
    2018: "https://doi.org/10.25984/1784729",
}

# verified working 2025-01-22 via
# $ wget "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool" -O foo.html -U "Mozilla/5.0 Catalyst/2025 Cooperative/2025"
HEADERS = {"User-Agent": "Mozilla/5.0 Catalyst/2025 Cooperative/2025"}


class DoeLeadArchiver(AbstractDatasetArchiver):
    """DOE LEAD archiver."""

    name = "doelead"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download DOE LEAD resources.
        
        The DOE LEAD Tool doesn't provide direct access to the raw data, but instead links to the current raw data release hosted on OEDI. It does not provide links to past data releases. So, we hard-code the DOIs for all known releases, archive those, but also check the DOE LEAD Tool page to see if there's a new release we don't know about yet.
        """
        # e.g.: https://data.openei.org/submissions/6219
        currentrelease_link_pattern = re.compile(r"data\.openei\.org/submissions")
        """Regex for matching the current raw data release on the DOE LEAD Tool page"""
        
        doi_link_pattern = re.compile(r"https://doi.org")
        """Regex for matching the DOI of the OEDI submission"""
        
        # e.g.: https://data.openei.org/files/6219/DC-2022-LEAD-data.zip
        #       https://data.openei.org/files/6219/Data%20Dictionary%202022.xlsx
        #       https://data.openei.org/files/6219/LEAD%20Tool%20States%20List%202022.xlsx
        data_link_pattern = re.compile(r"([^/]+(\d{4})(?:-LEAD-data.zip|.xlsx))")
        """Regex for matching the data files in a release on the OEDI page. Captures the year, and supports both .zip and .xlsx file names."""
        
        currentrelease_link = await self.get_hyperlinks(
            TOOL_URL, currentrelease_link_pattern, headers=HEADERS
        )
        if len(currentrelease_link) != 1:
            raise AssertionError(
                f"We expect exactly one outgoing link to data.openei.org/submissions at {TOOL_URL}, but we found: {currentrelease_link}"
            )
        currentrelease_link = currentrelease_link.pop()
        currentrelease_doi = await self.get_hyperlinks(currentrelease_link, doi_link_pattern)
        if len(currentrelease_doi) != 1:
            raise AssertionError(
                f"We expect exactly one DOI link at {currentrelease_link}, but we found: {currentrelease_doi}"
            )
        currentrelease_doi = currentrelease_doi.pop()
        
        currentrelease_found = False
        for year, doi in YEARS_DOIS.items():
            self.logger.info(f"Processing DOE LEAD raw data release for {year}: {doi}")
            if doi == currentrelease_doi:
                currentrelease_found = True
            filenames_links = {}
            for data_link in await self.get_hyperlinks(doi, data_link_pattern):
                matches = data_link_pattern.search(data_link)
                if not matches:
                    continue
                link_year = int(matches.group(2))
                if link_year != year:
                    raise AssertionError(
                        f"We expect all files at {doi} to be for {year}, but we found: {link_year} from {data_link}"
                    )
                filenames_links[matches.group(1)] = data_link
            if filenames_links:
                self.logger.info(f"Downloading: {year}, {len(filenames_links)} items")
                yield self.get_year_resource(filenames_links, year)
        if not currentrelease_found:
            raise AssertionError(
                f"New DOE LEAD raw data release detected at {currentrelease_doi}. Update the archiver to process it."
            )

    async def get_year_resource(self, links: dict[str, str], year: int) -> ResourceInfo:
        """Download all available data for a year.
        
        Resulting resource contains one zip file of CSVs per state/territory, plus a handful of .xlsx dictionary and geocoding files.
        
        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
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

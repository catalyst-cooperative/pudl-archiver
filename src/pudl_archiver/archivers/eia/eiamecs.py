"""Archive EIA Manufacturing Energy Consumption Survey (MECS)."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/consumption/manufacturing/data"

TABLE_LINK_PATTERNS: dict[str | int, str] = {
    # This includes a pattern for the standard recent data: e.g., Table1_1.xlsx or RSETable1_1.xls
    # as well as a pattern for the preliminary tables: e.g., Table%201_5%20Preliminary_2022.xlsx
    "recent": r"(rse|)table(?: |)(\d{1,2}|\d{1.1})_(\d{1,2})(?: preliminary_\d{4}|)(.xlsx|.xls)",
    2002: r"(rse|)table(\d{1,2}).(\d{1,2})_\d{1,2}(.xlsx|.xls)",
    # These earlier years the pattern is functional but not actually very informative.
    # so we will just use the original name by making the whole pattern a match
    1998: r"((d|e)\d{2}([a-z]\d{1,2})_(\d{1,2})(.xlsx|.xls))",
    1994: r"((rse|)m\d{2}_(\d{2})([a-d]|)(.xlsx|.xls))",
    1991: r"((rse|)mecs(\d{2})([a-z]|)(.xlsx|.xls))",
}
"""Dictionary of years or "recent" as keys and table link patterns as values.

From 2006 and forward the link pattern is the same but all of the older years
have bespoke table link patterns. The groups to match in the regex patterns
will be used to rename the files for the archives. The order of those match
groups indicate various things:

* first group: whether the file contains only Relative Standard Errors (RSE)
* second group: the major table number
* third group: the minor table number
* forth group: the file extension

The years from 1998 and back have table link patterns that could be used in this
same format with 4 match groups, but the major and minor table numbers are not
actually stored in the file name. So for these older years we've turned the whole
pattern into a group and use that (the original file name) as the stored name in
the archive.
"""


class EiaMECSArchiver(AbstractDatasetArchiver):
    """EIA MECS archiver."""

    name = "eiamecs"
    concurrency_limit = 5  # Number of files to concurrently download

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-MECS resources."""
        years_url = "https://www.eia.gov/consumption/data.php#mfg"
        year_link_pattern = re.compile(r"(manufacturing\/data\/)(\d{4})\/$")

        links = await self.get_hyperlinks(years_url, year_link_pattern)
        links.update({"/consumption/manufacturing/data/1991/": "1991 data"})
        expected_years = {
            1991,
            1994,
            1998,
            2002,
            2006,
            2010,
            2014,
            2018,
            2022,
        }  # Years of MECS data we expect
        # If any of the years in the links differs from those we expect, raise an assertion error.
        # A new year of data seems to correspond with burying the links of older datasets on the EIA MECS page,
        # so we want to investigate this manually when it happens.
        if not all(
            any(str(year) in link for link in links.values()) for year in expected_years
        ):
            raise AssertionError(
                f"Expected years {sorted(expected_years)}, found years {sorted(links.values())}"
            )

        for link in links:
            match = year_link_pattern.search(link)
            year = match.groups()[1]
            if self.valid_year(year):
                yield self.get_year_resources(year)

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all excel tables for a year."""
        self.logger.info(f"Attempting to find resources for: {year}")
        data_paths_in_archive = set()
        year_url = f"{BASE_URL}/{year}"
        zip_path = self.download_directory / f"eiamecs-{year}.zip"
        max_old_year = max(
            [year for year in TABLE_LINK_PATTERNS if isinstance(year, int)]
        )
        if int(year) > max_old_year:
            table_link_pattern = re.compile(
                TABLE_LINK_PATTERNS["recent"], re.IGNORECASE
            )
        else:
            table_link_pattern = re.compile(
                TABLE_LINK_PATTERNS[int(year)], re.IGNORECASE
            )
        # Loop through all download links for tables
        links = await self.get_hyperlinks(year_url, table_link_pattern)
        if not links:
            raise AssertionError(
                f"Expected to identify matching links in {year_url} but found none. Investigate this page!"
            )
        for table_link in links:
            # strip bc there is at least one file which has a space at the end of
            # the href link
            table_link = f"{year_url}/{table_link}".strip()
            self.logger.info(f"Fetching {table_link}")
            # We are going to rename the files in a standard format by extracting
            # patterns from the table_link_pattern
            # From 1998 and before there are a bunch of letters in the file names
            # in patterns that are probably parsable somehow, but for now we are
            # just going to keep the original file names
            match = table_link_pattern.search(table_link)
            filename = match.group(1)
            if int(year) > 1998:
                is_rse = match.group(1)
                # there are several ways the they indicate that the files are
                # "data" vs "rse". we will add this to the end of the file name
                # but only for rse bc for many years data and the rse are together
                rse_map = {"": "", "d": "", "RSE": "-rse", "rse": "-rse", "e": "-rse"}
                rse = rse_map[is_rse]
                major_num = match.group(2)
                minor_num = match.group(3)
                extension = match.group(4)
                # If the file is preliminary, note this in the constructed file name
                preliminary = (
                    "-preliminary" if "preliminary" in match.string.lower() else ""
                )
                # Download filename
                filename = f"eia-mecs-{year}-table-{major_num}-{minor_num}{rse}{preliminary}{extension}"
            download_path = self.download_directory / filename

            user_agent = self.get_user_agent()
            await self.download_file(
                table_link, download_path, headers={"User-Agent": user_agent}
            )
            # there are a small-ish handful of files who's links redirect to the main
            # mecs page. presumably its a broken link. we want to skip those files,
            # so we are going to check to see if the doctype of the bytes of the file
            # are html. if so we move on, otherwise add to the archive
            with Path.open(download_path, "rb") as f:
                first_bytes = f.read(20)
                if b"html" in first_bytes.lower().strip():
                    self.logger.warning(
                        f"Skipping {table_link} because it appears to be a redirect/html page."
                    )
                    pass
                else:
                    self.add_to_archive(
                        zip_path=zip_path,
                        filename=filename,
                        blob=download_path.open("rb"),
                    )
                    data_paths_in_archive.add(filename)
                    # Don't want to leave multiple files on disk, so delete
                    # immediately after they're safely stored in the ZIP
                    download_path.unlink()

        resource_info = ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=sorted(data_paths_in_archive)),
        )
        return resource_info

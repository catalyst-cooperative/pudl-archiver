"""Archive EIA Short-Term Energy Outlook (STEO).

There are two pages containing EIA STEO data:
* The archives, which contain a PDF with all charts and figures and an Excel sheet with
the base data, updated monthly.
* A page containing the current month's charts and figures. Note that some tables
may appear listed on this page but not in the aggregate data/figure downloads (e.g.,
winter fuel supplementary data), so we want to grab all tables here and not just the
aggregated table downloads.
"""

import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/outlooks/steo/"

# The URL containing former STEO data archives.
ARCHIVES_URL = "https://www.eia.gov/outlooks/steo/outlook.php"
# The URL containing the most recent STEO data.
CURRENT_URL = "https://www.eia.gov/outlooks/steo/data.php"

# Prior to 1997 the STEO was quarterly.
# We assign these partitions to the first month of the quarter.
STEO_ARCHIVE_DATES = set(
    [
        str(q).lower()
        for q in pd.period_range(start="1983-01", end="1996-12", freq="Q").asfreq(
            "M", how="S"
        )
    ]
    +
    # The EIA STEO is released monthly.
    [
        str(q).lower()
        for q in pd.period_range(start="1997-01", end=pd.to_datetime("today"), freq="M")
    ]
)


class EiaSteoArchiver(AbstractDatasetArchiver):
    """EIA STEO archiver."""

    name = "eiasteo"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA STEO resources."""
        # We grab data from the archive URL and the current URL separately.
        # For each month, make a zipfile containing all relevant data from the
        # current and/or the archival URL.

        archival_soup = await self.get_soup(ARCHIVES_URL)
        current_soup = await self.get_soup(CURRENT_URL)

        archival_links = [
            link
            for link in archival_soup.find_all("a", href=True)
            if (".pdf" in link["href"] or ".xls" in link["href"])
            and ("archives" in link["href"])
        ]
        # Create a dictionary mapping URLs to the corresponding year_month partition.
        # We use the href to get the file name rather than the description of the link
        # as sometimes the link description has extra whitespace, asterisks, etc.

        # All the links with "Q" are formatted 1Q89.pdf, e.g..
        # We get the file name using getText(), drop the file extension and then
        # format the year and month using pd.to_datetime().
        # Note that pd.to_datetime() infers these as being 20XX-Q1, instead of 19XX-Q1,
        # so we take the year and the month from datetime and manually append it to the
        # correct century
        quarterly_regex = re.compile(r"^archives\/(\d{1}Q\d{2})(?:\w*).(?:[a-zA-z]*)$")
        quarterly_archival_links = {
            link["href"]: "19"
            + pd.to_datetime(quarterly_regex.search(link["href"]).group(1)).strftime(
                "%y-%m"
            )
            for link in archival_links
            if "Q" in link["href"]
        }
        # All the other links can be parsed to map the filename to a year-month (e.g.,
        # sep09_filename.xls and sep09.pdf into 2009-09 partitions).
        year_month_regex = re.compile(
            r"^archives\/([a-zA-Z]{3}\d{2})(?:\w*).(?:[a-zA-z]*)$"
        )
        monthly_archival_links = {
            link["href"]: pd.to_datetime(
                year_month_regex.search(link["href"]).group(1), format="%b%y"
            ).strftime("%Y-%m")
            for link in archival_links
            if "Q" not in link["href"]
        }

        # Get the date of release for the current links from the website
        try:
            release_header = current_soup.find("div", class_="pub_title bg_aeo")
            release_header = release_header.find("strong")
        except AttributeError:
            raise AttributeError(
                f"Release date not found on site. Check bs4 configuration and {CURRENT_URL}. Changing your VPN might also help!"
            )

        assert release_header.text == "Release Date:"  # Check this is the one we want
        release_date = release_header.next_sibling  # Get date after header
        release_date = release_date.get_text(strip=True)
        # Split on any non-date characters (e.g., tab, new line) and grab the first set of text
        release_date = re.split("[^a-zA-Z0-9, ]", release_date)[0].strip()
        current_partition = pd.to_datetime(release_date).strftime("%Y-%m")

        current_links = [
            link
            for link in current_soup.find_all("a", href=True)
            if any(ext in link["href"] for ext in [".pdf", ".xlsx", ".png"])
        ]
        current_links = {link["href"]: current_partition for link in current_links}

        all_links = pd.Series(
            {**quarterly_archival_links, **monthly_archival_links, **current_links}
        )

        # Check that all partitions of data we've found correspond to the
        # partitions of data we expect.
        assert all(partition in STEO_ARCHIVE_DATES for partition in all_links), (
            f"Got unexpected partition from link: {[partition for partition in all_links if partition not in STEO_ARCHIVE_DATES]}"
        )

        # Saving files by month will result in exceeding Zenodo's 100 file limit
        # so we zip all files by year.
        steo_years = {
            date[0:4] for date in STEO_ARCHIVE_DATES
        }  # Get years from quarters
        for year in sorted(steo_years):
            if self.valid_year(year):
                yield self.get_year_resources(all_links=all_links, year=year)

    async def get_year_resources(
        self, all_links=pd.Series, year=str
    ) -> list[ResourceInfo]:
        """Download all files from a year."""
        data_paths_in_archive = set()
        zip_path = self.download_directory / f"eiasteo-{year}.zip"

        year_month_partitions = [
            year_month
            for year_month in STEO_ARCHIVE_DATES
            if year_month.startswith(year)
        ]
        self.logger.info(
            f"Downloading {len(year_month_partitions)} months of {year} data."
        )

        for year_month in sorted(year_month_partitions):
            # Write a folder in the zipfile for each year_month
            links = all_links[all_links == year_month]
            self.logger.info(f"Downloading {len(links)} files from {year_month}.")
            for link in links.keys():  # noqa: SIM118
                file_url = urljoin(BASE_URL, link)
                # Get filename from URL
                filename = Path(urlparse(file_url).path).name
                download_path = self.download_directory / filename
                folder_path = f"{year_month}/{filename}"  # Put file in a folder for each year-month
                await self.download_file(file_url, download_path)
                self.add_to_archive(
                    zip_path=zip_path,
                    filename=folder_path,
                    blob=download_path.open("rb"),
                )
                data_paths_in_archive.add(folder_path)
                # Don't want to leave multiple files on disk, so delete
                # immediately after they're safely stored in the ZIP
                download_path.unlink()

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": int(year)},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

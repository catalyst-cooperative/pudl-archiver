"""Archive EIA Short-Term Energy Outlook (STEO)."""

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

ARCHIVES_URL = "https://www.eia.gov/outlooks/steo/outlook.php"
CURRENT_URL = "https://www.eia.gov/outlooks/steo/data.php"

# Prior to 1997 the STEO was quarterly.
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
        # For each month, make a zipfile containing all relevant data from either the
        # current or the archival URL

        archival_soup = await self.get_soup(ARCHIVES_URL)
        current_soup = await self.get_soup(CURRENT_URL)

        archival_links = [
            link
            for link in archival_soup.find_all("a", href=True)
            if (".pdf" in link["href"] or ".xls" in link["href"])
            and ("archives" in link["href"])
        ]
        # Create a dictionary containing year_month partition and link href.
        # All the links with "Q" are formatted 1Q89.pdf, e.g..
        # to_datetime infers these as being 2089-Q1, so we take the year and the month
        # from datetime and manually append it to the 1900 prefix.
        # All the other links can be parsed to map sep09_filename.xls and sep09.pdf
        # into 2009-09 partitions. We slice the first 5 characters to drop additional
        # file name information.
        # We use a dictionary here because some hrefs are duplicated, and should only
        # have one partition per href.
        archival_links = {
            link["href"]: pd.to_datetime(
                link.getText().split(".")[0][0:5], format="%b%y"
            ).strftime("%Y-%m")
            if "Q" not in link["href"]
            else "19" + pd.to_datetime(link.getText().split(".")[0]).strftime("%y-%m")
            for link in archival_links
        }

        # Get the date of release for the current links from the website
        release_header = current_soup.find("div", class_="pub_title bg_aeo").find(
            "strong"
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

        all_links = pd.Series({**archival_links, **current_links})

        # Saving files by month will result in exceeding Zenodo's 100 file limit
        # so we zip all files by year.
        steo_years = {
            date[0:4] for date in STEO_ARCHIVE_DATES
        }  # Get years from quarters
        for year in steo_years:
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

        for year_month in year_month_partitions:
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

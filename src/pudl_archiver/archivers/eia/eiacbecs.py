"""Archive EIA  Commercial Buildings Energy Consumption Survey (CBECS)."""

import re
from pathlib import Path
from urllib.parse import urljoin

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://www.eia.gov/consumption/commercial/data/"


class EiaCbecsArchiver(AbstractDatasetArchiver):
    """EIA CBECS archiver."""

    name = "eiacbecs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-CBECS resources."""
        # we use this link and pattern to determine which years of CBECS data exists,
        # but these base year links are only a portion of the view links so we
        # construct the full links within get_year_resources
        link_pattern = re.compile(r"commercial/data/(\d{4})/$", re.IGNORECASE)
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            match = link_pattern.search(link)
            year = match.group(1)
            if int(year) > 2018:
                raise self.logger.warning(
                    f"There is a new year of data: {year}! This will almost certainly "
                    "require some updating of this archive."
                )
            yield self.get_year_resources(year)

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all files from all views for a year."""
        data_paths_in_archive = set()
        zip_path = self.download_directory / f"eiacbecs-{year}.zip"
        char_and_cons_pattern = (
            rf"(?:{year}|archive)(?:.*)/([a-z,\d]{{1,8}})(.xls|.xlsx|.pdf)$"
        )
        data_view_patterns = {
            "characteristics": re.compile(char_and_cons_pattern),
            "consumption": re.compile(char_and_cons_pattern),
            # some of the mircodata links are like csv/file01.csv which doesn't include
            # the year or archive. instead of adding a null option for that first group
            # we add a whole new pattern for these two years because if we don't
            # we'd pick up some of the 2018 pdf files that are on the right hand side
            # of these pages
            "microdata": re.compile(
                rf"(?:{year}/|archive/)(?:xls|pdf|csv|sas)/(.*)(.xls|.xlsx|.pdf|.csv|.exe|.zip)$"
                if year not in ["2003", "1999"]
                else r"^(?:csv|pdf)/(.*)(.csv|.pdf)$"
            ),
            # the most recent cbecs doesn't a year or archive in the methodology links
            # BUT there are almost always pdf files from 2018 that get caught up in
            # these scrapers if we don't include year or archive. so we have a special
            # 2018 pattern
            "methodology": re.compile(
                rf"(?:{year}|archive/pubs)(?:/pdf|)/(.*)(.pdf$)"
                if year != "2018"
                else r"/consumption/commercial(?:/data/2018|)/pdf/(.*)(.pdf)$"
            ),
        }

        for view, table_link_pattern in data_view_patterns.items():
            year_url = f"{BASE_URL}{year}/index.php?view={view}"
            for link in await self.get_hyperlinks(year_url, table_link_pattern):
                match = table_link_pattern.search(link)
                unique_id = (
                    match.group(1).replace("_", "-").replace(" ", "-").lower().strip()
                )
                file_extension = match.group(2)
                filename = f"eiacbecs-{year}-{view}-{unique_id}{file_extension}"
                file_url = urljoin(year_url, link)
                download_path = self.download_directory / filename
                await self.download_file(file_url, download_path)
                # there are a small-ish handful of files who's links redirect to the main
                # cbecs page. presumably its a broken link. we want to skip those files,
                # so we are going to check to see if the doctype of the bytes of the file
                # are html. if so we move on, otherwise add to the archive
                with Path.open(download_path, "rb") as f:
                    first_bytes = f.read(20)
                    if b"html" in first_bytes.lower().strip():
                        self.logger.warning(
                            f"Skipping {file_url} because it appears to be a redirect/html page."
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
        # Check if all of the views found any links
        year_has_all_views: dict[str, bool] = {
            view: any(fn for fn in data_paths_in_archive if view in fn)
            for view in data_view_patterns
        }
        views_without_files = [
            view for (view, has_files) in year_has_all_views.items() if not has_files
        ]
        if views_without_files:
            raise AssertionError(
                "We expect all years of EIA CBECS to have some data from all four "
                f"views, but we found these views without files for {year}: {views_without_files}"
            )

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

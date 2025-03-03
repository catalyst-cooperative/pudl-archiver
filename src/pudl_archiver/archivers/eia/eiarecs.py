"""Archive EIA Residential Energy Consumption Survey (RECS)."""

import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin, urlparse

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import is_html_file

BASE_URL = "https://www.eia.gov/consumption/residential/data/"


@dataclass(frozen=True)
class TabInfo:
    """Information needed to archive the links in a tab."""

    url: str
    name: str
    year: int


class EiaRECSArchiver(AbstractDatasetArchiver):
    """EIA RECS archiver."""

    name = "eiarecs"
    base_url = "https://www.eia.gov/consumption/residential/data/2020/"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-RECS resources.

        Looks in the "data" dropdown in the navbar for links to each year.
        """
        soup = await self.get_soup(self.base_url)
        years = soup.select("div.subnav div.dat_block a")
        numbered_years = [y for y in years if y.text.strip().lower() != "previous"]

        for year in numbered_years:
            if self.valid_year(year.text.strip()):
                yield self.__get_year_resources(
                    url=urljoin(self.base_url, year["href"]),
                    year=int(year.text.strip()),
                )

    async def __get_year_resources(self, url: str, year: int) -> ResourceInfo:
        """Download all data files for a year.

        Finds links to all available tabs, then dispatches each tab to a
        handler, which downloads content from the tabs and adds it to the
        year's zip archive.

        Tab handlers are mostly the same for each tab across the years, but
        there is an ability to add exceptions when necessary.

        Each year's actual forms are also archived - these are mostly not
        linked from the tabs themselves, so we go to the main survey page and
        download them there.

        Args:
            url: a string that represents the base page for this year
            year: the actual year number we are archiving

        Returns:
            ResourceInfo: information about this year's zip file & its contents.
        """
        self.logger.info(f"Starting {year}")

        tab_infos = await self.__select_tabs(url)

        tab_handlers_overrides = {"methodology": {2009: self.__skip, 2015: self.__skip}}

        zip_path = self.download_directory / f"eiarecs-{year}.zip"
        paths_within_archive = []
        for tab in tab_infos:
            tab_handler = tab_handlers_overrides.get(tab.name, {}).get(
                tab.year, self.__get_tab_html_and_links
            )
            paths_within_archive += await tab_handler(tab_info=tab, zip_path=zip_path)

        self.logger.info(f"Looking for original forms for {year}")
        original_forms_within_archive = await self.__get_original_forms(year, zip_path)

        self.logger.info(f"Got original forms for {year}, returning ResourceInfo list.")
        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(
                file_paths=paths_within_archive + original_forms_within_archive
            ),
        )

    async def __add_links_to_archive(
        self, url_paths: dict[str, str], zip_path: Path
    ) -> list[str]:
        """Download and add link contents to a zipfile.

        Skips links that lead to HTML content since these are usually broken links.

        Args:
            url_paths: mapping from URLs to the filenames we want them to have
                in the zip.
            zip_path: path to the archive

        Returns:
            list[str]: the filepaths, relative to the archive root, that we
                just added.
        """
        data_paths_in_archive = []
        for link, output_filename in url_paths.items():
            download_path = self.download_directory / output_filename
            self.logger.debug(f"Fetching {link} to {download_path}")
            await self.download_file(link, download_path, timeout=120)
            with download_path.open("rb") as f:
                # TODO 2025-02-04: check html-ness against the suffix... if we
                # have a php/html/cfm/etc. we probably actually *do* want the
                # html file.
                if is_html_file(f):
                    self.logger.info(f"{link} was HTML file - skipping.")
                    continue
                self.add_to_archive(
                    zip_path=zip_path,
                    filename=output_filename,
                    blob=f,
                )
                self.logger.debug(f"Added {link} to {zip_path} as {output_filename}")
                data_paths_in_archive.append(output_filename)
            download_path.unlink()
        return data_paths_in_archive

    async def __get_tab_links(self, tab_info: TabInfo, zip_path: Path) -> list[str]:
        """Get the data files for a single tab.

        First, gets a list of all of the <a> tags within the tab contents which have an href attribute.

        These tag objects have the HTML attrs accessible as if they were dictionaries - href, src, etc.

        They also have some Python attributes of their own that you can read: text, contents, children, etc.

        See https://beautiful-soup-4.readthedocs.io/en/latest/#tag for details.
        """
        soup = await self.get_soup(tab_info.url)
        links_in_tab = soup.select("div.tab-contentbox a[href]")
        log_scope = f"{tab_info.year}:{tab_info.name}"
        self.logger.info(f"{log_scope}: Found {len(links_in_tab)} links")

        links_filtered = [
            link
            for link in links_in_tab
            if not (
                "mailto" in link["href"].lower() or "all tables" in link.text.lower()
            )
        ]

        self.logger.info(f"{log_scope}: Found {len(links_filtered)} relevant links")

        resolved_links = [
            urljoin(tab_info.url, link["href"]) for link in links_filtered
        ]
        links_with_filenames = {
            link: f"eiarecs-{tab_info.year}-{tab_info.name}-{self.__get_filename_from_link(link)}"
            for link in resolved_links
        }

        data_paths = await self.__add_links_to_archive(
            links_with_filenames, zip_path=zip_path
        )

        self.logger.info(
            f"{log_scope}: Added {len(links_with_filenames)} links to archive"
        )

        return data_paths

    async def __get_tab_html_and_links(
        self, tab_info: TabInfo, zip_path: Path
    ) -> list[str]:
        """Get the data files in the tab, *and* get the tab content itself.

        First, get all the links within the tab that aren't HTML files and
        aren't mailtos.

        Then, gets the entire HTML contents of div.tab-contentbox, which
        contains the tab contents.

        Then, makes a new HTML document with an html and a body tag, and shoves
        the old tab contents in there.

        This makes a new HTML file that can be opened by one's browser and
        includes the tab's contents - but any links/images will not work.
        """
        log_scope = f"{tab_info.year}:{tab_info.name}"
        self.logger.info(f"{log_scope}: Getting links in tab")
        links = await self.__get_tab_links(tab_info=tab_info, zip_path=zip_path)

        soup = await self.get_soup(tab_info.url)
        tab_content = soup.select_one("div.tab-contentbox")
        self.logger.info(f"{log_scope}: Got {len(tab_content)} bytes of tab content")
        html = soup.new_tag("html")
        body = soup.new_tag("body")
        html.append(body)
        body.append(tab_content)
        # TODO 2025-02-03: consider using some sort of html-to-pdf converter here.
        # use html-sanitizer or something before feeding it into pdf.

        filename = f"eiarecs-{tab_info.year}-{tab_info.name}-tab-contents.html"
        self.add_to_archive(
            zip_path=zip_path,
            filename=filename,
            blob=BytesIO(html.prettify().encode("utf-8")),
        )
        self.logger.info(f"{log_scope}: Added html to {zip_path} under {filename}")
        return links + [filename]

    async def __get_original_forms(self, year: int, zip_path: Path) -> list[str]:
        """Get the survey forms that were used to collect the data.

        These are all on the same page, which is different from the yearly RECS
        archive pages, so we do this separately from all the tab content above.
        """
        forms_url = "https://www.eia.gov/survey/"
        soup = await self.get_soup(forms_url)
        all_links = soup.select("#eia-457 div.expand-collapse-content a[href]")
        links_filtered = [
            link for link in all_links if f"/archive/{year}" in link["href"]
        ]

        resolved_links = [urljoin(forms_url, link["href"]) for link in links_filtered]

        links_with_filenames = {
            link: f"eiarecs-{year}-form-{self.__get_filename_from_link(link)}"
            for link in resolved_links
        }

        return await self.__add_links_to_archive(
            links_with_filenames, zip_path=zip_path
        )

    def __get_filename_from_link(self, url: str) -> str:
        filepath = Path(urlparse(url).path)
        stem = re.sub(r"\W+", "-", filepath.stem)
        return f"{stem}{filepath.suffix}".lower()

    async def __select_tabs(self, url: str) -> set[TabInfo]:
        """Get the clickable tab links from the EIA RECS page layout."""

        async def get_unselected_tabs(url):
            soup = await self.get_soup(url)
            unselected_tabs = soup.select("#tab-container a")
            year = int(re.search(r"\d{4}", url)[0])
            return {
                TabInfo(
                    url=urljoin(url, tab["href"]),
                    name=re.sub(r"\W+", "-", tab.text.strip()).lower(),
                    year=year,
                )
                for tab in unselected_tabs
            }

        first_unselected_tabs = await get_unselected_tabs(url)
        another_tab_url = next(iter(first_unselected_tabs)).url
        next_unselected_tabs = await get_unselected_tabs(another_tab_url)
        return first_unselected_tabs.union(next_unselected_tabs)

    async def __skip(self, **kwargs) -> list[str]:
        return []

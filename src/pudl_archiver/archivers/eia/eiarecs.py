"""Archive EIA Residential Energy Consumption Survey (RECS)."""

import logging
import re
from dataclasses import dataclass
from io import BytesIO
from urllib.parse import urljoin

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout


@dataclass
class LinkSet:
    """Information a set of links in one tab of the RECS viewer.

    See https://www.eia.gov/consumption/residential/data/2020/.
    """

    url: str
    short_name: str
    pattern: re.Pattern


def _url_for(year: int, view: str):
    """Get the URL for a specific RECS year/tab combo."""
    return (
        f"https://www.eia.gov/consumption/residential/data/{year}/index.php?view={view}"
    )


YEAR_LINK_SETS = {
    2020: {
        "housing_characteristics": LinkSet(
            url=_url_for(year=2020, view="characteristics"),
            short_name="hc",
            pattern=re.compile(r"HC (\d{1,2}\.\d{1,2})\.(xlsx)"),
        ),
        "consumption & expenditures": LinkSet(
            url=_url_for(year=2020, view="consumption"),
            short_name="ce",
            pattern=re.compile(r"ce(\d\.\d{1,2}[a-z]?)\.(xlsx)"),
        ),
        "state data (housing characteristics)": LinkSet(
            url=_url_for(year=2020, view="state"),
            short_name="state",
            pattern=re.compile(r"State (.*)\.(xlsx)"),
        ),
        "state data (consumption & expenditures)": LinkSet(
            url=_url_for(year=2020, view="state"),
            short_name="state-ce",
            pattern=re.compile(r"ce(\d\.\d{1,2}\..*)\.(xlsx)"),
        ),
        "microdata": LinkSet(
            url=_url_for(year=2020, view="microdata"),
            short_name="microdata",
            pattern=re.compile(r"(recs.*public.*)\.(csv)"),
        ),
        "methodology": LinkSet(
            url=_url_for(year=2020, view="methodology"),
            short_name="methodology",
            pattern=re.compile(r"pdf/(.+)\.(pdf)"),
        ),
    }
}
logger = logging.getLogger(f"catalystcoop.{__name__}")


class EiaRECSArchiver(AbstractDatasetArchiver):
    """EIA RECS archiver."""

    name = "eiarecs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-RECS resources."""
        for year in [2020]:
            yield self.get_year_resources(year)

    def __is_html_file(self, fileobj: BytesIO) -> bool:
        header = fileobj.read(30).lower().strip()
        fileobj.seek(0)
        return b"<!doctype html>" in header

    async def get_year_resources(self, year: int) -> list[ResourceInfo]:
        """Download all excel tables for a year."""
        # Loop through all download links for tables
        tables = []
        zip_path = self.download_directory / f"eia-recs-{year}.zip"
        data_paths_in_archive = set()
        # Loop through different categories of data (all .xlsx)
        link_sets = YEAR_LINK_SETS[year]
        for link_set in link_sets.values():
            for table_link in await self.get_hyperlinks(link_set.url, link_set.pattern):
                table_link = urljoin(link_set.url, table_link)
                logger.info(f"Fetching {table_link}")
                match = link_set.pattern.search(table_link)
                matched_metadata = (
                    match.group(1).replace(".", "-").replace(" ", "_").lower()
                )
                matched_format = match.group(2)
                output_filename = f"eia-recs-{year}-{link_set.short_name}-{matched_metadata}.{matched_format}"

                # Download file
                download_path = self.download_directory / output_filename
                await self.download_file(table_link, download_path)
                with download_path.open("rb") as f:
                    if self.__is_html_file(f):
                        continue
                    self.add_to_archive(
                        zip_path=zip_path,
                        filename=output_filename,
                        blob=f,
                    )
                data_paths_in_archive.add(output_filename)
                download_path.unlink()

        tables.append(
            ResourceInfo(
                local_path=zip_path,
                partitions={"year": year},
                layout=ZipLayout(file_paths=data_paths_in_archive),
            )
        )
        return tables

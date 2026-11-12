"""Download Bureau of Labor Statistics (BLS) Consumer Price Index (CPI) data."""

import asyncio
import datetime
import json
import os
from typing import ClassVar

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

"https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CPIAUCSL&scale=left&cosd=1947-01-01&coed=2026-10-01&line_color=%230073e6&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2026-11-11&revision_date=2026-11-11&nd=1947-01-01"


class BlsCpiArchiver(AbstractDatasetArchiver):
    """BLS CPI archiver."""

    name = "blscpi"
    concurrency_limit = 1  # Cranky old server

    start_year = 1990  # Last major revision was in 1987, this seems like a reasonable history to start.
    end_year = datetime.datetime.now().year

    base_url = "https://api.bls.gov/publicAPI/v2"
    parameters: ClassVar[dict[str, str | None]] = {
        "registrationkey": os.environ.get("BLS_API_KEY", ""),
    }  # Set to API key

    async def get_resources(self) -> ArchiveAwaitable:
        """Download BLS CPI resources."""
        years = sorted(
            [
                year
                for year in range(self.start_year, self.end_year + 1)
                if self.valid_year(year)
            ]
        )
        year_chunks = [years[i : i + 10] for i in range(0, len(years), 10)]
        for chunk in year_chunks:
            yield self.get_year_resource(chunk)

    async def get_year_resource(self, years: list[int]) -> ResourceInfo:
        """Download all available data for a year.

        Args:
            year: the year we're downloading data for
        """
        self.logger.info(f"Archiving BLS data for {years}")
        series_ids = [
            "CUUR0000SA0",
            "CUSR0000SA0",
        ]  # Which series to archive - in the future, could expand to multiple,
        start_year = years[0]
        end_year = years[-1]

        archive_path = (
            self.download_directory / f"{self.name}-{start_year}-{end_year}.zip"
        )
        response = await self.get_json(
            f"{self.base_url}/timeseries/data/",
            json=self.parameters
            | {
                "startyear": str(start_year),
                "endyear": str(end_year),
                "seriesid": series_ids,
                "annualaverage": True,
            },
            headers={"Content-type": "application/json"},
            post=True,
        )

        json_name = f"{self.name}-{start_year}-{end_year}.json"
        json_path = self.download_directory / json_name

        with json_path.open("w", encoding="utf-8") as json_file:
            json.dump(response, json_file, indent=2)

        with json_path.open("rb") as json_file:
            self.add_to_archive(
                zip_path=archive_path,
                filename=json_name,
                blob=json_file,
            )

        await asyncio.sleep(60)  # This is really a cranky server.

        return ResourceInfo(
            local_path=archive_path,
            partitions={"year": years, "series": series_ids},
            layout=ZipLayout(file_paths={json_path.name}),
        )

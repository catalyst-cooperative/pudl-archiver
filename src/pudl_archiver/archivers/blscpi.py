"""Download Bureau of Labor Statistics (BLS) Consumer Price Index (CPI) data."""

import datetime
import os
from typing import ClassVar

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

"https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CPIAUCSL&scale=left&cosd=1947-01-01&coed=2026-10-01&line_color=%230073e6&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2026-11-11&revision_date=2026-11-11&nd=1947-01-01"


class BlsCpiArchiver(AbstractDatasetArchiver):
    """BLS CPI archiver."""

    name = "blscpi"
    start_year = 1990  # Last major revision was in 1987, this seems like a reasonable history to start.
    end_year = datetime.datetime.now().year

    base_url = "https://api.bls.gov/publicAPI/v2"
    parameters: ClassVar[dict[str, str | None]] = {
        "registrationkey": os.environ.get("BLS_API_KEY", ""),
    }  # Set to API key

    async def get_resources(self) -> ArchiveAwaitable:
        """Download BLS CPI resources."""
        for year in range(self.start_year, self.end_year + 1):
            yield self.get_year_resource(year)

    async def get_year_resource(self, year: int) -> ResourceInfo:
        """Download all available data for a year.

        Args:
            year: the year we're downloading data for
        """
        response = await self.get_json(
            f"{self.base_url}/timeseries/data/",
            data=self.parameters
            | {
                "startyear": year,
                "endyear": year + 1,
                "seriesid": "LAUCN040010000000005",
            },
            headers={"Content-type": "application/json"},
        )
        # TODO actually archive data....
        if response:
            pass

        return ResourceInfo()

"""Scrapy spider for downloading historical FERC Form 2 Visual FoxPro data."""
from pathlib import Path

import scrapy
from scrapy.http import Request

from pudl_scrapers import items
from pudl_scrapers.helpers import new_output_dir


class Ferc2Spider(scrapy.Spider):
    """Scrapy spider for downloading historical FERC Form 2 Visual FoxPro data."""

    name = "ferc2"
    allowed_domains = ["www.ferc.gov"]
    start_urls = [
        "https://www.ferc.gov/industries-data/natural-gas/industry-forms/form-2-2a-3-q-gas-historical-vfp-data"
    ]

    def start_requests(self):
        """Start requesting FERC 2 forms.

        Yields:
            List of Requests for FERC 2 forms
        """
        # Spider settings are not available during __init__, so finalizing here
        settings_output_dir = Path(self.settings.get("OUTPUT_DIR"))
        output_root = settings_output_dir / "ferc2"
        self.output_dir = new_output_dir(output_root)

        yield from self.all_form_requests()

    def parse(self, response):
        """Produce the FERC 2 item.

        Args:
            response: scrapy.http.Response containing FERC Form 2 data

        Yields:
            FERC Form 2 item
        """
        if response.meta["part"] is None:
            path = self.output_dir / f"ferc2-{response.meta['year']}.zip"
        else:
            path = (
                self.output_dir
                / f"ferc2-{response.meta['year']}-{response.meta['part']}.zip"
            )

        yield items.Ferc2(
            data=response.body,
            year=response.meta["year"],
            part=response.meta["part"],
            save_path=path,
        )

    def form_for_year_part(self, year: int, part: int | None = None):
        """Produce a form request for the given year.

        Args:
            year: Reporting year of the data to be scraped.
            part: In earlier years data is split into two parts (A-M, and N-Z) which
                must be downloaded separately. Part indicates which one to get.

        Returns:
            Request for the FERC 2 form

        """
        early_urls: dict[tuple(int, int), str] = {
            (1991, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y91A-M.zip",
            (1991, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y91N-Z.zip",
            (1992, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y92A-M.zip",
            (1992, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y92N-Z.zip",
            (1993, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y93A-M.zip",
            (1993, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y93N-Z.zip",
            (1994, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y94A-M.zip",
            (1994, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y94N-Z.zip",
            (1995, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y95A-M.zip",
            (1995, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y95N-Z.zip",
            (1996, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y96-1.zip",
            (1996, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y96-2.zip",
            (1997, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y97-1.zip",
            (1997, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y97-2.zip",
            (1998, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y98-1.zip",
            (1998, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y98-2.zip",
            (1999, 1): "https://www.ferc.gov/sites/default/files/2020-07/F2Y99-1.zip",
            (1999, 2): "https://www.ferc.gov/sites/default/files/2020-07/F2Y99-2.zip",
        }
        # Special rules for grabbing the early two-part data:
        if part is not None:
            assert year >= 1991 and year <= 1999  # nosec: B101
            url = early_urls[(year, part)]
        else:
            assert year >= 1996 and year <= 2021  # nosec: B101
            url = f"https://forms.ferc.gov/f2allyears/f2_{year}.zip"

        return Request(
            url,
            meta={"year": year, "part": part},
            callback=self.parse,
        )

    def all_form_requests(self):
        """Produces form requests for all supported years.

        Yields:
            Requests for all available FERC Form 2 zip files
        """
        for year in range(1991, 2000):
            for part in [1, 2]:
                yield self.form_for_year_part(year=year, part=part)

        for year in range(1996, 2022):
            yield self.form_for_year_part(year=year, part=None)

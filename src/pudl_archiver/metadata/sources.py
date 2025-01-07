"""Metadata and operational constants."""

from typing import Any

from pudl.metadata.constants import CONTRIBUTORS, LICENSES

NON_PUDL_SOURCES: dict[str, Any] = {
    "eiamecs": {
        "title": "EIA Manufacturing Energy Consumption Survey",
        "path": "https://www.eia.gov/consumption/manufacturing/data/2018/",
        "description": (
            "EIA Form 846 A and B is more commonly known as the Manufacturing Energy",
            "Consumption Survey (MECS). MECS is a national sample survey that collects",
            "information on the stock of U.S. manufacturing establishment, their",
            "energy-related building characteristics, and their energy consumption",
            "and expenditures. MECS is conducted every four years.",
        ),
        "working_partitions": {
            1991,
            1994,
            1998,
            2002,
            2006,
            2010,
            2014,
            2018,
        },  # Census DP1 is monolithic.
        "keywords": sorted(
            {
                "manufacturing",
                "MECS",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
}

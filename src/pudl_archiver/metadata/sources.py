"""Metadata and operational constants."""

from typing import Any

from pudl.metadata.constants import CONTRIBUTORS, LICENSES

# To add a new contributor, follow the following format to add an entry to the
# ADDL_CONTRIBUTORS dictionary below formatted like this:
#     "name-shorthand": {
#         "title": "Catalyst Cooperative",
#         "email": "pudl@catalyst.coop",
#         "path": "https://catalyst.coop",
#         "role": "publisher",
#         "zenodo_role": "distributor",
#         "organization": "Catalyst Cooperative",
#         "orcid": "0000-1234-5678-9101"
#     }
# Note that the only required fields are title (your name) and path
# (e.g., a link to your Github account, your ORCID site or a personal webpage), but
# filling other fields is strongly encouraged!
ADDL_CONTRIBUTORS: dict[str, dict[str, str]] = {}

NON_PUDL_SOURCES: dict[str, Any] = {
    "eiamecs": {
        "title": "EIA Manufacturing Energy Consumption Survey",
        "path": "https://www.eia.gov/consumption/manufacturing/data/2018/",
        "description": (
            "EIA Form 846 A and B is more commonly known as the Manufacturing Energy"
            "Consumption Survey (MECS). MECS is a national sample survey that collects"
            "information on the stock of U.S. manufacturing establishment, their"
            "energy-related building characteristics, and their energy consumption"
            "and expenditures. MECS is conducted every four years."
        ),
        "working_partitions": {
            "years": [1991, 1994, 1998, 2002, 2006, 2010, 2014, 2018]
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

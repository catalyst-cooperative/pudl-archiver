"""Constant metadata values referenced in various data sources."""

from typing import Any

LICENSES: dict[str, dict[str, str]] = {
    "cc-by-4.0": {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0",
    },
    "cc-zero": {
        "name": "CC0-1.0",
        "title": "CC0 1.0 Universal",
        "path": "https://creativecommons.org/publicdomain/zero/1.0/",
    },
    "us-govt": {
        "name": "other-pd",
        "title": "U.S. Government Work",
        "path": "http://www.usa.gov/publicdomain/label/1.0/",
    },
}
"""Static license descriptors (frictionless License fields: name, title, path).

Keyed by the short identifiers used throughout the archiver and PUDL repos.
"""

KEYWORDS: dict[str, list[str]] = {
    "electricity": [
        "electricity",
        "electric",
        "generation",
        "energy",
        "utility",
        "transmission",
        "distribution",
        "kWh",
        "MWh",
        "kW",
        "MW",
        "kilowatt hours",
        "kilowatts",
        "megawatts",
        "megawatt hours",
        "power",
    ],
    "eia": [
        "eia",
        "energy information administration",
    ],
    "us_govt": [
        "united states",
        "us",
        "usa",
        "government",
        "federal",
    ],
}
"""Topical lists of keywords used in this repository (copied from the PUDL datapackage metadata)."""

CONTRIBUTORS: dict[str, dict[str, Any]] = {
    "catalyst-cooperative": {
        "name": "catalyst-cooperative",
        "title": "Catalyst Cooperative",
        "email": "pudl@catalyst.coop",
        "path": "https://catalyst.coop",
        "roles": ["publisher"],
        "zenodo_role": "distributor",
        "organization": "Catalyst Cooperative",
    }
}

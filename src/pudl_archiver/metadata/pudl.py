"""Lazy loader for the PUDL datapackage descriptor."""

import functools
import json
import os

from upath import UPath

PUDL_DATAPACKAGE_S3 = "s3://pudl.catalyst.coop/nightly/pudl_parquet_datapackage.json"

# Static license descriptors (frictionless License fields: name, title, path).
# Keyed by the short identifiers used throughout the archiver.
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


@functools.lru_cache(maxsize=1)
def get_pudl_datapackage() -> dict:
    """Return the PUDL datapackage descriptor, loading it exactly once.

    Checks the ``PUDL_DATAPACKAGE_PATH`` environment variable first; if set, reads from
    that local path.  Otherwise fetches from the public S3 URL
    :data:`PUDL_DATAPACKAGE_S3`.

    Raises:
        RuntimeError: if the descriptor cannot be read from either source.
    """
    raw_path = os.getenv("PUDL_DATAPACKAGE_PATH", PUDL_DATAPACKAGE_S3)
    path = (
        UPath(raw_path, anon=True) if raw_path.startswith("s3://") else UPath(raw_path)
    )

    try:
        return json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load PUDL datapackage from {path}. "
            f"Set $PUDL_DATAPACKAGE_PATH to a local path as an alternative."
        ) from exc


@functools.lru_cache(maxsize=1)
def get_pudl_sources() -> dict[str, dict]:
    """Return PUDL data sources keyed by name.

    Replaces ``pudl.metadata.sources.SOURCES``.
    Each value is a dict with at minimum: ``name``, ``title``, ``path``,
    ``description``, ``keywords``.
    """
    return {source["name"]: source for source in get_pudl_datapackage()["sources"]}


@functools.lru_cache(maxsize=1)
def get_pudl_contributors() -> dict[str, dict]:
    """Return PUDL contributors keyed by slugified organization name.

    Replaces ``pudl.metadata.constants.CONTRIBUTORS``.
    Keys are derived from the ``name`` field.

    """
    return {
        contributor["name"]: contributor
        for contributor in get_pudl_datapackage()["contributors"]
    }

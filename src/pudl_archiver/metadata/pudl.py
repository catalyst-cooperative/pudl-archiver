"""Lazy loader for the PUDL datapackage descriptor."""

import functools
import json
import os

from upath import UPath

PUDL_DATAPACKAGE_S3 = "s3://pudl.catalyst.coop/nightly/pudl_parquet_datapackage.json"
FERCEQR_DATAPACKAGE_S3 = "s3://pudl.catalyst.coop/ferceqr/datapackage.json"


def _load_descriptor(env_var: str, default: str, label: str) -> dict:
    """Load a datapackage descriptor from ``$env_var`` if set, else ``default``."""
    raw_path = os.getenv(env_var, default)
    path = (
        UPath(raw_path, anon=True) if raw_path.startswith("s3://") else UPath(raw_path)
    )
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load {label} datapackage from {path}. "
            f"Set ${env_var} to a local path as an alternative."
        ) from exc


@functools.lru_cache(maxsize=1)
def get_pudl_datapackage() -> dict:
    """Return the PUDL datapackage descriptor, loading it exactly once.

    Checks the ``PUDL_DATAPACKAGE_PATH`` environment variable first; if set, reads from
    that local path.  Otherwise fetches from the public S3 URL
    :data:`PUDL_DATAPACKAGE_S3`.

    Raises:
        RuntimeError: if the descriptor cannot be read from either source.
    """
    return _load_descriptor("PUDL_DATAPACKAGE_PATH", PUDL_DATAPACKAGE_S3, "PUDL")


@functools.lru_cache(maxsize=1)
def get_ferceqr_datapackage() -> dict:
    """Return the FERC EQR datapackage descriptor, loading it exactly once.

    The FERC EQR is deployed separately from the rest of PUDL, with its own
    descriptor. Checks ``FERCEQR_DATAPACKAGE_PATH`` first, then falls back to
    :data:`FERCEQR_DATAPACKAGE_S3`.

    Raises:
        RuntimeError: if the descriptor cannot be read from either source.
    """
    return _load_descriptor(
        "FERCEQR_DATAPACKAGE_PATH", FERCEQR_DATAPACKAGE_S3, "FERC EQR"
    )


def _descriptors() -> list[dict]:
    return [get_pudl_datapackage(), get_ferceqr_datapackage()]


@functools.lru_cache(maxsize=1)
def get_pudl_sources() -> dict[str, dict]:
    """Return PUDL data sources keyed by name.

    Merges the top-level ``sources`` of the PUDL and FERC EQR descriptors, which are
    published separately. Replaces ``pudl.metadata.sources.SOURCES``. Each value is a
    dict with at minimum: ``name``, ``title``, ``path``, ``description``, ``keywords``.
    """
    return {
        source["name"]: source
        for descriptor in _descriptors()
        for source in descriptor.get("sources", [])
    }


@functools.lru_cache(maxsize=1)
def get_pudl_contributors() -> dict[str, dict]:
    """Return PUDL contributors keyed by slugified organization name.

    Replaces ``pudl.metadata.constants.CONTRIBUTORS``. Merged across both descriptors.
    """
    return {
        contributor["name"]: contributor
        for descriptor in _descriptors()
        for contributor in descriptor.get("contributors", [])
    }

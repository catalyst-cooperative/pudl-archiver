"""Interact with data depositions in all the backends we use.

Such as Zenodo, Zenodo, and Zenodo.
"""

from typing import Any

import aiohttp

from pudl_archiver.frictionless import DataPackage
from pudl_archiver.utils import RunSettings

from . import fsspec
from .depositor import (
    DEPOSITION_BACKENDS,
    DepositionAction,
    DepositionChange,
    DepositorAPIClient,
    DraftDeposition,
    PublishedDeposition,
)
from .zenodo import depositor


async def get_deposition(
    dataset: str,
    session: aiohttp.ClientSession,
    run_settings: RunSettings,
) -> tuple[DraftDeposition, DataPackage | None]:
    """Create draft deposition from scratch or previous version."""
    deposition_backend = DEPOSITION_BACKENDS[run_settings.depositor]
    api_client = await deposition_backend.api_client.initialize_client(
        session=session,
        **run_settings.depositor_args,
    )
    if run_settings.initialize:
        return await deposition_backend.draft_interface.new_draft(
            dataset_id=dataset,
            settings=run_settings,
            api_client=api_client,
        ), None

    published_deposition = (
        await deposition_backend.published_interface.get_most_recent_version(
            dataset_id=dataset,
            settings=run_settings,
            api_client=api_client,
        )
    )

    original_datapackage_bytes = await published_deposition.get_file("datapackage.json")
    original_datapackage = DataPackage.model_validate_json(original_datapackage_bytes)

    return await published_deposition.open_draft(), original_datapackage

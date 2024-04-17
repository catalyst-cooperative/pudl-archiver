"""Interact with data depositions in all the backends we use.

Such as Zenodo, Zenodo, and Zenodo.
"""

from dataclasses import dataclass

import aiohttp

from pudl_archiver.frictionless import DataPackage
from pudl_archiver.utils import RunSettings

from .depositor import (
    DepositionAction,
    DepositionChange,
    DepositorAPIClient,
    DraftDeposition,
    PublishedDeposition,
)


@dataclass
class DepositionBackend:
    """Wrap Published and Draft Deposition classes for a single depositor."""

    api_client: type[DepositorAPIClient]
    published_interface: type[PublishedDeposition]
    draft_interface: type[DraftDeposition]


DEPOSITION_BACKENDS: dict[str, DepositionBackend] = {}


def register_depositor(
    depositor_name: str,
    api_client: type[DepositorAPIClient],
    published_interface: type[PublishedDeposition],
    draft_interface: type[DraftDeposition],
):
    """Function to register an implementation of the depositor interface."""
    DEPOSITION_BACKENDS[depositor_name] = DepositionBackend(
        api_client=api_client,
        published_interface=published_interface,
        draft_interface=draft_interface,
    )


async def get_deposition(
    dataset: str, session: aiohttp.ClientSession, run_settings: RunSettings
) -> tuple[DraftDeposition, DataPackage | None]:
    """Create draft deposition from scratch or previous version."""
    deposition_backend = DEPOSITION_BACKENDS[run_settings.depositor]
    api_client = await deposition_backend.api_client.initialize_client(
        session, run_settings.sandbox
    )
    if run_settings.initialize:
        deposition = await api_client.create_new_deposition(dataset)
        return deposition_backend.draft_interface(
            dataset_id=dataset,
            settings=run_settings,
            api_client=api_client,
            deposition=deposition,
        ), None
    deposition = await api_client.get_deposition(dataset)

    published_deposition = deposition_backend.published_interface(
        dataset_id=dataset,
        settings=run_settings,
        api_client=api_client,
        deposition=deposition,
    )

    original_datapackage_bytes = await published_deposition.get_file("datapackage.json")
    original_datapackage = DataPackage.model_validate_json(original_datapackage_bytes)

    return await published_deposition.open_draft(), original_datapackage

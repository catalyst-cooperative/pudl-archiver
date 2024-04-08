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
    DraftDeposition,
    PublishedDeposition,
)


@dataclass
class DepositionInterface:
    """Wrap Published and Draft Deposition classes for a single depositor."""

    published_interface: type[PublishedDeposition]
    draft_interface: type[DraftDeposition]


DEPOSITION_INTERFACES: dict[str, DepositionInterface] = {}


def register_depositor(
    depositor_name: str,
    published_interface: type[PublishedDeposition],
    draft_interface: type[DraftDeposition],
):
    """Function to register an implementation of the depositor interface."""
    DEPOSITION_INTERFACES[depositor_name] = DepositionInterface(
        published_interface=published_interface, draft_interface=draft_interface
    )


async def get_deposition(
    dataset: str, session: aiohttp.ClientSession, run_settings: RunSettings
) -> tuple[DraftDeposition, DataPackage | None]:
    """Create draft deposition from scratch or previous version."""
    deposition_interface = DEPOSITION_INTERFACES[run_settings.depositor]
    if run_settings.initialize:
        return await deposition_interface.draft_interface.initialize_from_scratch(
            dataset, session, run_settings
        ), None

    published_deposition = (
        await deposition_interface.published_interface.get_latest_version(
            dataset, session, run_settings
        )
    )

    original_datapackage_bytes = await published_deposition.get_file("datapackage.json")
    original_datapackage = DataPackage.model_validate_json(original_datapackage_bytes)

    return await published_deposition.open_draft(), original_datapackage

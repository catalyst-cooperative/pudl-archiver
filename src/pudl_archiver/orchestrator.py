"""Core routines for archiving raw data packages."""
import logging

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors.depositor import Depositor
from pudl_archiver.depositors.zenodo.depositor import ZenodoDepositorInterface
from pudl_archiver.utils import RunSettings

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    session: aiohttp.ClientSession,
    settings: RunSettings,
) -> RunSummary:
    """Use downloader and depositor to archive a dataset."""
    depositor = Depositor.get_latest_version(
        interface=ZenodoDepositorInterface,
        dataset=dataset,
        session=session,
        settings=settings,
    )

    resources = {}
    original_datapackage = depositor.get_file("datapackage.json")
    async with depositor.open_draft() as draft:
        async for name, resource in downloader.downlad_all_resources([]):
            resources[name] = resource
            draft.add_resource(name, resource)

        # Delete files in draft that weren't downloaded by downloader
        for filename in draft.list_files():
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                draft.delete_file(filename)

        # Add datapackage after all resources are added to draft
        new_datapackage = draft.attach_datapackage(resources)

        # Validate run
        validations = downloader.validate_dataset(
            original_datapackage, new_datapackage, resources
        )
        summary = RunSummary(
            dataset,
            original_datapackage,
            new_datapackage,
            validations,
            draft.get_deposition_link(),
        )
        draft.validate_run(summary)
    return summary

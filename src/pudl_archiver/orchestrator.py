"""Core routines for archiving raw data packages."""
import logging

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors.depositor import Depositor

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    depositor: Depositor,
    session: aiohttp.ClientSession,
) -> RunSummary:
    """Use downloader and depositor to archive a dataset."""
    resources = {}
    original_datapackage = await depositor.get_file("datapackage.json")
    async with depositor.open_draft() as draft:
        async for name, resource in downloader.download_all_resources([]):
            resources[name] = resource
            await draft.add_resource(name, resource)

        # Delete files in draft that weren't downloaded by downloader
        for filename in await draft.list_files():
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                draft.delete_file(filename)

        # Create new datapackage
        new_datapackage = await draft.attach_datapackage(
            resources, original_datapackage
        )

        # Validate run
        validations = downloader.validate_dataset(
            original_datapackage, new_datapackage, resources
        )
        summary = RunSummary.create_summary(
            dataset,
            original_datapackage,
            new_datapackage,
            validations,
            draft.get_deposition_link(),
        )
        draft.validate_run(summary)
        return summary

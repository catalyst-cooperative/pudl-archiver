"""Core routines for archiving raw data packages."""

import logging

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors import PublishedDeposition, get_deposition
from pudl_archiver.utils import RunSettings

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    run_settings: RunSettings,
    session: aiohttp.ClientSession,
) -> tuple[RunSummary, PublishedDeposition | None]:
    """Use downloader and depositor to archive a dataset."""
    resources = {}
    # Get datapackage from previous version if there is one
    draft, original_datapackage = await get_deposition(dataset, session, run_settings)
    async for name, resource in downloader.download_all_resources():
        resources[name] = resource
        draft = await draft.add_resource(name, resource)

    # Delete files in draft that weren't downloaded by downloader
    for filename in await draft.list_files():
        if filename not in resources and filename != "datapackage.json":
            logger.info(f"Deleting {filename} from deposition.")
            draft = await draft.delete_file(filename)

    # Create new datapackage
    new_datapackage = await draft.attach_datapackage(resources, original_datapackage)

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
    published = await draft.publish_if_valid(
        summary,
        run_settings.clobber_unchanged,
        run_settings.auto_publish,
    )
    return summary, published

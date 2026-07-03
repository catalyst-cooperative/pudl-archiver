"""Core routines for archiving raw data packages."""

import logging

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary, exception_validation
from pudl_archiver.depositors import PublishedDeposition, get_deposition
from pudl_archiver.frictionless import Partitions
from pudl_archiver.utils import RunSettings

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    run_settings: RunSettings,
    session: aiohttp.ClientSession,
    skip_partitions: dict[str, Partitions] = {},
) -> tuple[RunSummary, PublishedDeposition | None]:
    """Use downloader and depositor to archive a dataset."""
    resources = {}
    # Get datapackage from previous version if there is one
    draft, original_datapackage = await get_deposition(dataset, session, run_settings)

    # Download resources and add to archive
    run_exception = None
    try:
        async for name, resource in downloader.download_all_resources(
            skip_partitions.values(),
        ):
            resources[name] = resource
            draft = await draft.add_resource(name, resource)
    except Exception as e:
        run_exception = e
        logger.error(f"download_all_resources failed!\n{e}")

    # Delete files in draft that weren't downloaded by downloader
    for filename in await draft.list_files():
        if (
            filename not in resources
            and filename != "datapackage.json"
            and filename not in skip_partitions
        ):
            logger.info(f"Deleting {filename} from deposition.")
            draft = await draft.delete_file(filename)

    # Create new datapackage
    draft, new_datapackage = await draft.attach_datapackage(
        partitions_in_deposition={
            name: resource.partitions for name, resource in resources.items()
        }
        | skip_partitions
    )

    # Validate run
    validations = downloader.validate_dataset(
        original_datapackage, new_datapackage, resources
    )
    validations.append(exception_validation(run_exception))
    summary = RunSummary.create_summary(
        name=dataset,
        baseline_datapackage=original_datapackage,
        new_datapackage=new_datapackage,
        validation_tests=validations,
        record_url=draft.get_deposition_link(),
        failed_partitions=downloader.failed_partitions,
        successful_partitions={
            name: resource.partitions
            for name, resource in resources.items()
            if name not in downloader.failed_partitions
        }
        | skip_partitions,
        run_settings=run_settings,
    )
    published = await draft.publish_if_valid(
        summary,
        run_settings.clobber_unchanged,
        run_settings.auto_publish,
    )
    return summary, published

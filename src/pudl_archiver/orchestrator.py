"""Core routines for archiving raw data packages."""

import json
import logging
from pathlib import Path

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors import PublishedDeposition, get_deposition
from pudl_archiver.frictionless import Partitions
from pudl_archiver.utils import RunSettings

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _get_partitions_from_previous_run(
    run_summary_json: str | None,
) -> tuple[dict[str, Partitions], dict[str, Partitions]]:
    """Return failed/successful partitions from previous run if requested.

    In order to retry a previous failed run, we save the failed and
    successful partitions from that run in the run summary json file
    that the archiver outputs. This allows us to only re-run the partitions
    that failed. If ``run_summary_json`` is None, then this function will
    return two empty dict's.
    """
    failed_partitions, successful_partitions = {}, {}
    if run_summary_json is not None:
        with Path(run_summary_json).open() as f:
            run_summary = RunSummary.model_validate(json.load(f)[0])
            failed_partitions = run_summary.failed_partitions
            successful_partitions = run_summary.successful_partitions
    return failed_partitions, successful_partitions


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

    # Get partitions from previous run if retrying a run
    failed_partitions, successful_partitions = _get_partitions_from_previous_run(
        run_settings.retry_run
    )

    async for name, resource in downloader.download_all_resources(
        list(failed_partitions.values()),
    ):
        resources[name] = resource
        draft = await draft.add_resource(name, resource)

    # Delete files in draft that weren't downloaded by downloader
    for filename in await draft.list_files():
        if (
            filename not in resources
            and filename != "datapackage.json"
            and filename not in successful_partitions
        ):
            logger.info(f"Deleting {filename} from deposition.")
            draft = await draft.delete_file(filename)

    # Create new datapackage
    new_datapackage = await draft.attach_datapackage(
        partitions_in_deposition={
            name: resource.partitions for name, resource in resources.items()
        }
        | successful_partitions
    )

    # Validate run
    validations = downloader.validate_dataset(
        original_datapackage, new_datapackage, resources
    )
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
        },
    )
    published = await draft.publish_if_valid(
        summary,
        run_settings.clobber_unchanged,
        run_settings.auto_publish,
    )
    return summary, published

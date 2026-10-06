"""Core routines for archiving raw data packages."""

import logging
import tempfile
from pathlib import Path

import aiohttp
from upath import UPath

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import (
    MetadataArchiveSummary,
    RunSummary,
    _datapackage_changed,
    exception_validation,
)
from pudl_archiver.depositors import (
    DepositionAction,
    DepositionChange,
    DraftDeposition,
    PublishedDeposition,
    get_deposition,
)
from pudl_archiver.frictionless import DataPackage, Partitions, ResourceInfo
from pudl_archiver.utils import RunSettings

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def _remove_files_and_describe(
    draft: DraftDeposition,
    resources: dict[str, ResourceInfo],
    skip_partitions: dict[str, Partitions],
) -> tuple[DraftDeposition, DataPackage]:
    """Make the draft hold what was archived, and attach a datapackage describing it.

    Files of the previous version that weren't downloaded are left out of the new
    version, which only matters if it's published. This is also where a draft left
    by a failed run, which this run has now completed, stops being marked incomplete.
    """
    draft = await draft.clear_incomplete_marker()
    removed = [
        filename
        for filename in await draft.list_files()
        if filename not in resources
        and filename != "datapackage.json"
        and filename not in skip_partitions
    ]
    if removed:
        logger.info(
            f"Leaving {len(removed)} files out of the new version of the deposition: "
            f"{', '.join(removed)}"
        )
    for filename in removed:
        draft = await draft.delete_file(filename)

    draft, new_datapackage = await draft.attach_datapackage(
        partitions_in_deposition={
            name: resource.partitions for name, resource in resources.items()
        }
        | skip_partitions
    )
    return draft, new_datapackage


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    run_settings: RunSettings,
    session: aiohttp.ClientSession,
    skip_partitions: dict[str, Partitions] | None = None,
) -> tuple[RunSummary, PublishedDeposition | None]:
    """Use downloader and depositor to archive a dataset.

    Args:
        dataset: Name of the dataset.
        downloader: Archiver to find and download the files with.
        run_settings: Settings of the run.
        session: HTTP session.
        skip_partitions: Partitions, by filename, that a previous run archived, and
            which this run, a retry, doesn't need to download again.
    """
    resources = {}
    # Get datapackage from previous version if there is one
    draft, original_datapackage = await get_deposition(dataset, session, run_settings)
    skip_partitions = skip_partitions or {}

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
        logger.exception("Error downloading resources")

    if run_exception is not None:
        # The new version is incomplete, and we don't know what it should contain, so
        # don't guess. Removing the files that weren't downloaded would also leave a
        # datapackage.json that doesn't describe them, and a cascade of failed
        # validations about files missing or the archive shrinking, which are just
        # consequences of the failure. Whatever was uploaded stays in the draft, for a
        # retry to build on, but the draft mixes new and old files, so it is marked
        # incomplete, and loses the datapackage.json of the previous version.
        logger.error(
            "The new version of the archive is incomplete, so leaving the files in "
            "the draft as they are and marking it incomplete."
        )
        # Describe the draft before marking it, as the marker isn't part of the archive
        new_datapackage = original_datapackage or draft.generate_datapackage(
            {name: resource.partitions for name, resource in resources.items()}
            | skip_partitions
        )
        draft = await draft.mark_incomplete(
            f"{type(run_exception).__name__}: {run_exception}"
        )
    else:
        draft, new_datapackage = await _remove_files_and_describe(
            draft, resources, skip_partitions
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


async def orchestrate_metadata_archive(
    dataset: str,
    source_path: str,
    run_settings: RunSettings,
    session: aiohttp.ClientSession,
) -> MetadataArchiveSummary | None:
    """Archive only the ``datapackage.json`` of an fsspec archive on Zenodo.

    Some datasets are too big for Zenodo, so their data is archived with the fsspec
    depositor. Zenodo then holds just the metadata, which provides a citable record
    and a place to fetch the datapackage. This reads the not-yet-published
    ``datapackage.json`` from the fsspec ``workspace`` directory, stamps it with the
    version and DOI of a new Zenodo draft, uploads it to that draft, and writes the
    stamped copy back to the workspace so both locations agree. The draft is never
    published here; that requires manual review.

    Args:
        dataset: Name of the dataset.
        source_path: Path of the fsspec deposition (containing ``workspace``).
        run_settings: Settings for the Zenodo depositor.
        session: HTTP session.

    Returns:
        A summary of the draft, or None if there is no new datapackage to archive.
    """
    datapackage_path = UPath(source_path) / "workspace" / "datapackage.json"
    if not datapackage_path.exists():
        logger.info(
            f"No unpublished datapackage.json found at {datapackage_path}, "
            "so there is no new metadata to archive."
        )
        return None
    datapackage = DataPackage.model_validate_json(datapackage_path.read_bytes())

    draft, original_datapackage = await get_deposition(dataset, session, run_settings)

    # Identify the new Zenodo version, and stamp it on the datapackage.
    doi = f"https://doi.org/{draft.reserved_doi}"
    version = draft.deposition.metadata.version
    datapackage = datapackage.model_copy(update={"version": version, "id_": doi})

    if original_datapackage is not None and not _datapackage_changed(
        original_datapackage, datapackage
    ):
        logger.info("Datapackage is unchanged from Zenodo version, deleting draft.")
        await draft.delete_deposition()
        return None

    datapackage_bytes = datapackage.model_dump_json(by_alias=True, indent=4).encode()
    action = (
        DepositionAction.UPDATE
        if "datapackage.json" in await draft.list_files()
        else DepositionAction.CREATE
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "datapackage.json"
        local_path.write_bytes(datapackage_bytes)
        draft = await draft._apply_change(
            DepositionChange(
                action_type=action, name="datapackage.json", resource=local_path
            )
        )

    # Keep the fsspec copy in sync, now that it is on Zenodo.
    datapackage_path.write_bytes(datapackage_bytes)

    logger.info(
        f"Created Zenodo draft {draft.get_deposition_link()} for {dataset} "
        f"version {version} ({doi}). Review and publish it manually."
    )
    return MetadataArchiveSummary(
        dataset_name=dataset,
        record_url=draft.get_deposition_link(),
        version=version,
        doi=doi,
        datapackage_changed=True,
    )

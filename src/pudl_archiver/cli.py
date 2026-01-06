"""A script for archiving raw PUDL data on Zenodo."""

import asyncio
import json
import logging
from pathlib import Path

import click
import coloredlogs
from dotenv import load_dotenv

from pudl_archiver import ARCHIVERS, archive_dataset
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.utils import RunSettings

logger = logging.getLogger("catalystcoop.pudl_archiver")


@click.group
def pudl_archiver():
    """Top level pudl_archiver command."""
    load_dotenv()
    logger.setLevel(logging.INFO)
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=logging.INFO, logger=logger)


@pudl_archiver.command
def list_datasets():
    """Command to list all datasets for which there is an archiver."""
    for name in sorted(ARCHIVERS.keys()):
        print(name)


@pudl_archiver.group
def archive():
    """Group for archive commands."""
    pass


# Define a set of shared options for the archive group of commands
initialize_option = click.option(
    "--initialize",
    is_flag=True,
    help="Initialize new deposition by reserving a DOI. If used with the fsspec"
    " depositor, this will try to create the directory at the specified deposition-path.",
)
auto_publish_option = click.option(
    "--auto-publish",
    is_flag=True,
    help="Automatically publish a deposition, rather than requiring manual review before publishing.",
)
clobber_unchanged_option = click.option(
    "--clobber-unchanged",
    is_flag=True,
    help="Delete draft deposition if unchanged.",
)
refresh_metadata_option = click.option(
    "--refresh-metadata",
    is_flag=True,
    help="Regenerate metadata from PUDL data source rather than existing archived metadata.",
)
only_years_option = click.option(
    "--only-years",
    "-y",
    multiple=True,
    type=int,
    help="Years to download data for. Supported datasets: censusdp1tract, censuspep, "
    "eia176, eia191, eia757a, eia860, eia860m, eia861, eia923, eia930, "
    "eiaaeo, eiamecs, eiawater, eiasteo, epacamd_eia, epacems, epaegrid, ferc1, ferc2, "
    "ferc6, ferc60, ferc714, mshamines, nrelatb, phmsagas, usgsuswtdb",
)
dataset_argument = click.argument("dataset", type=str)


@archive.command
@click.option("--sandbox", is_flag=True, help="Use Zenodo sandbox server")
@initialize_option
@auto_publish_option
@clobber_unchanged_option
@refresh_metadata_option
@only_years_option
@dataset_argument
def zenodo(
    sandbox: bool,
    initialize: bool,
    auto_publish: bool,
    clobber_unchanged: bool,
    refresh_metadata: bool,
    only_years: tuple[int],
    dataset: str,
):
    """Archive DATASET to zenodo."""
    asyncio.run(
        archive_dataset(
            dataset=dataset,
            run_settings=RunSettings(
                initialize=initialize,
                retry_run=None,
                refresh_metadata=refresh_metadata,
                auto_publish=auto_publish,
                clobber_unchanged=clobber_unchanged,
                summary_file=f"{dataset}_run_summary.json",
                only_years=only_years,
                depositor="zenodo",
                depositor_args={"sandbox": sandbox},
            ),
        )
    )


@archive.command
@initialize_option
@auto_publish_option
@clobber_unchanged_option
@refresh_metadata_option
@only_years_option
@dataset_argument
@click.argument(
    "deposition-path",
    type=str,
)
def fsspec(
    initialize: bool,
    auto_publish: bool,
    clobber_unchanged: bool,
    refresh_metadata: bool,
    only_years: tuple[int],
    dataset: str,
    deposition_path: str,
):
    """Archive DATASET to DEPOSITION_PATH.

    DEPOSITION_PATH is a configurable path used by the fsspec depositor that should
    be in an fsspec compatible format like: 'file://local/path/to/folder' or
    'file:///absolute/path/to/folder' or 'gs://path/to/gcs_bucket'
    """
    asyncio.run(
        archive_dataset(
            dataset=dataset,
            run_settings=RunSettings(
                initialize=initialize,
                retry_run=None,
                refresh_metadata=refresh_metadata,
                auto_publish=auto_publish,
                clobber_unchanged=clobber_unchanged,
                summary_file=f"{dataset}_run_summary.json",
                only_years=only_years,
                depositor="fsspec",
                depositor_args={"deposition_path": deposition_path},
            ),
        )
    )


@archive.command
@auto_publish_option
@click.argument(
    "summary-file",
    type=str,
)
def retry_run(summary_file: str, auto_publish: bool):
    """Retry a previous run of the archiver by passing a JSON summary file from the run.

    SUMMARY_FILE Points to a Run Summary JSON file output by a previous run which
    either failed, or which completed succesfully, but didn't publish its results.
    All run settings will be inherited from the previous run except for ``auto-publish``,
    which will be overridden by this CLI to avoid accidental publication on a retry.
    """
    # Load run summary file and parse
    with Path(summary_file).open() as f:
        failed_run_summary = RunSummary.model_validate(json.load(f))

    # Extract settings from failed run
    run_settings = failed_run_summary.run_settings.model_copy(
        update={
            "retry_run": summary_file,
            "auto_publish": auto_publish,
        },
    )

    # Find which partitions failed/succeeded in previous run
    failed_partitions = failed_run_summary.failed_partitions
    successful_partitions = failed_run_summary.successful_partitions

    # Retry archiver run
    asyncio.run(
        archive_dataset(
            dataset=failed_run_summary.dataset_name,
            run_settings=run_settings,
            failed_partitions=failed_partitions,
            successful_partitions=successful_partitions,
        )
    )


def main():
    """Kick off async script."""
    pudl_archiver()

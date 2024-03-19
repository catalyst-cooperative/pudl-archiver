"""A script for archiving raw PUDL data on Zenodo."""

import argparse
import asyncio
import logging
from pathlib import Path

import coloredlogs
from dotenv import load_dotenv

from pudl_archiver import ARCHIVERS, archive_datasets

logger = logging.getLogger("catalystcoop.pudl_archiver")


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Upload PUDL data archives to Zenodo")
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Name of the Zenodo deposition.",
        choices=list(ARCHIVERS.keys()),
    )
    parser.add_argument(
        "--only-years",
        nargs="*",
        help="Years to download data for. Supported datasets: censusdp1tract, eia176, "
        "eia191, eia757a, eia860, eia860m, eia861, eia923, eia_bulk_elec, eiaaeo, "
        "eiawater, epacamd_eia, epacems, ferc1, ferc2, ferc6, ferc60, ferc714, "
        "mshamines, phmsagas",
        type=int,
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all defined archivers.",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Use Zenodo sandbox server",
    )
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="Initialize new deposition by preserving a DOI",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip actually uploading to Zenodo",
    )
    parser.add_argument(
        "--summary-file",
        type=Path,
        help="Generate a JSON archive run summary",
    )
    parser.add_argument(
        "--auto-publish",
        action="store_true",
        help="Automatically publish a deposition, rather than requiring manual review before publishing.",
    )
    (
        parser.add_argument(
            "--download-dir",
            help="Directory to download files to. Use tmpdir if not specified.",
            default=None,
        ),
    )
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Regenerate metadata from PUDL data source rather than existing archived metadata.",
    )
    return parser.parse_args()


def main():
    """Run desired archivers."""
    load_dotenv()
    logger.setLevel(logging.INFO)
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=logging.INFO, logger=logger)
    args = parse_main()
    if args.all:
        args.datasets = ARCHIVERS.keys()
    del args.all
    asyncio.run(archive_datasets(**vars(args)))


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""A script for archiving raw PUDL data on Zenodo."""

import argparse
import asyncio
import logging

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
        type=str,
        help="Generate a JSON archive run summary",
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

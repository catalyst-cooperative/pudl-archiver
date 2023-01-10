#!/usr/bin/env python
"""A script for archiving raw PUDL data on Zenodo."""

import argparse
import asyncio
import logging
import os

import aiohttp
import coloredlogs
from dotenv import load_dotenv

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.zenodo.api_client import ZenodoClient

logger = logging.getLogger("catalystcoop.pudl_archiver")


import pathlib


def all_archivers():
    dirpath = pathlib.Path(__file__).parent
    pyfiles = [path.relative_to(dirpath) for path in dirpath.glob("archivers/**/*.py") if "__init__" != path.stem]
    module_names = [f"pudl_archiver.{str(p).replace('/', '.')[:-3]}" for p in pyfiles ]
    for module in module_names:
        __import__(module)
    return AbstractDatasetArchiver.__subclasses__()

ARCHIVERS = {archiver.name: archiver for archiver in all_archivers()}


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Upload PUDL data archives to Zenodo")
    parser.add_argument(
        "datasets",
        nargs="*",
        help="Name of the Zenodo deposition.",
        choices=list(ARCHIVERS.keys())
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
    return parser.parse_args()


async def archive_dataset(
    name: str,
    zenodo_client: ZenodoClient,
    session: aiohttp.ClientSession,
    initialize: bool = False,
):
    """Download and archive dataset on zenodo."""
    
    async with zenodo_client.deposition_interface(name, initialize) as deposition:
        # Create new deposition then return
        cls = ARCHIVERS.get(name)
        if not cls:
            raise RuntimeError("Dataset not supported")
        else:
            archiver = cls(session, deposition)
        await archiver.create_archive()


async def archive_datasets():
    """A CLI for the PUDL Zenodo Storage system."""
    args = parse_main()
    load_dotenv()

    if args.sandbox:
        upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
    else:
        upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_TOKEN_UPLOAD"]

    connector = aiohttp.TCPConnector(limit_per_host=20, force_close=True)
    async with aiohttp.ClientSession(
        connector=connector, raise_for_status=True
    ) as session:
        # List to gather all archivers to run asyncronously
        tasks = []
        for dataset in args.datasets:
            zenodo_client = ZenodoClient(
                "dataset_doi.yaml",
                session,
                upload_key,
                publish_key,
                testing=args.sandbox,
            )

            tasks.append(
                archive_dataset(
                    dataset, zenodo_client, session, initialize=args.initialize
                )
            )

        await asyncio.gather(*tasks)


def main():
    """Run desired archivers."""
    logger.setLevel(logging.INFO)
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=logging.INFO, logger=logger)

    asyncio.run(archive_datasets())


if __name__ == "main":
    main()

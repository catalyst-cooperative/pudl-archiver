#!/usr/bin/env python
"""A script for archiving raw PUDL data on Zenodo."""

import argparse
import asyncio
import logging
import os

import aiohttp
import coloredlogs
import yaml
from dotenv import load_dotenv

from pudl_scrapers.archiver.ferc1 import Ferc1Archiver
from pudl_scrapers.archiver.ferc6 import Ferc6Archiver
from pudl_scrapers.archiver.ferc60 import Ferc60Archiver
from pudl_scrapers.zenodo.api_client import ZenodoClient

logger = logging.getLogger("catalystcoop.pudl_scrapers")


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Upload PUDL data archives to Zenodo")
    parser.add_argument(
        "datasets",
        nargs="*",
        help="Name of the Zenodo deposition. Supported: censusdp1tract, "
        "eia860, eia861, eia923, eia_bulk_elec, epacems, epacamd_eia, "
        "ferc1, ferc2, ferc6, ferc60, ferc714, eia860m",
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
        match name:
            case "ferc1":
                archiver = Ferc1Archiver(session, deposition)
            case "ferc6":
                archiver = Ferc6Archiver(session, deposition)
            case "ferc60":
                archiver = Ferc60Archiver(session, deposition)
            case _:
                raise RuntimeError("Dataset not supported")

        await archiver.create_archive()


async def archive_datasets():
    """A CLI for the PUDL Zenodo Storage system."""
    args = parse_main()
    load_dotenv()

    if args.sandbox:
        upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
    else:
        upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]

    async with aiohttp.ClientSession(raise_for_status=True) as session:
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
    logger.setLevel(logging.INFO)
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=logging.INFO, logger=logger)

    asyncio.run(archive_datasets())


if __name__ == "main":
    main()

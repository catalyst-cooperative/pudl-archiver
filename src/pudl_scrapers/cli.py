#!/usr/bin/env python
"""A script for archiving raw PUDL data on Zenodo."""

import argparse
import asyncio
import logging
import os

import aiohttp
import yaml
from dotenv import load_dotenv

from pudl_scrapers.zenodo.api_client import DatasetSettings, ZenodoDepositionInterface

logger = logging.getLogger(__name__)
logging.basicConfig()


def parse_main():
    """Process base commands from the CLI."""
    parser = argparse.ArgumentParser(description="Upload PUDL data archives to Zenodo")
    parser.add_argument(
        "deposition",
        help="Name of the Zenodo deposition. Supported: censusdp1tract, "
        "eia860, eia861, eia923, eia_bulk_elec, epacems, epacamd_eia, "
        "ferc1, ferc2, ferc714, eia860m",
    )
    return parser.parse_args()


async def archive_dataset(
    name: str,
    doi: str,
    session: aiohttp.ClientSession,
    upload_key: str,
    publish_key: str,
    testing: bool = False,
):
    """Download and archive dataset on zenodo."""
    deposition = ZenodoDepositionInterface(
        session, upload_key, publish_key, doi, testing
    )


async def main():
    """A CLI for the PUDL Zenodo Storage system."""
    args = parse_main()
    load_dotenv()

    with open("dataset_doi.yaml") as f:
        dataset_settings = {
            name: DatasetSettings(**dois) for name, dois in yaml.safe_load(f)
        }

    async with aiohttp.ClientSession() as session:
        # List to gather all archivers to run asyncronously
        archivers = []
        for dataset in args.datasets:
            settings = dataset_settings.get(dataset, DatasetSettings())

            if args.testing:
                doi = settings.sandbox_doi
                upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
                publish_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
            else:
                doi = settings.sandbox_doi
                upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
                publish_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]

            if doi is None:
                raise RuntimeError(
                    f"No DOI available for {dataset}. Must reserve a DOI before running."
                )

            archivers.append(
                archive_dataset(
                    dataset, doi, session, upload_key, publish_key, args.testing
                )
            )

        await asyncio.gather(archivers)


asyncio.run(main())

"""Tool to download data resources and create archives on Zenodo for use in PUDL."""
import asyncio
import os
import pathlib

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.zenodo.api_client import ZenodoClient


def all_archivers():
    """List all Archivers that have been defined."""
    dirpath = pathlib.Path(__file__).parent
    pyfiles = [
        path.relative_to(dirpath)
        for path in dirpath.glob("archivers/**/*.py")
        if "__init__" != path.stem
    ]
    module_names = [f"pudl_archiver.{str(p).replace('/', '.')[:-3]}" for p in pyfiles]
    for module in module_names:
        # AbstractDatasetArchiver won't know about the subclasses unless they are imported
        __import__(module)
    return AbstractDatasetArchiver.__subclasses__()


ARCHIVERS = {archiver.name: archiver for archiver in all_archivers()}


async def archive_dataset(
    name: str,
    zenodo_client: ZenodoClient,
    session: aiohttp.ClientSession,
    initialize: bool = False,
    dry_run: bool = True,
):
    """Download and archive dataset on zenodo."""
    async with zenodo_client.deposition_interface(
        name, initialize, dry_run=dry_run
    ) as deposition:
        # Create new deposition then return
        cls = ARCHIVERS.get(name)
        if not cls:
            raise RuntimeError(f"Dataset {name} not supported")
        else:
            archiver = cls(session, deposition)
        await archiver.create_archive()


async def archive_datasets(
    datasets: list[str],
    sandbox: bool = True,
    initialize: bool = False,
    dry_run: bool = True,
):
    """A CLI for the PUDL Zenodo Storage system."""
    if sandbox:
        upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
    else:
        upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_TOKEN_PUBLISH"]

    connector = aiohttp.TCPConnector(limit_per_host=20, force_close=True)
    async with aiohttp.ClientSession(
        connector=connector, raise_for_status=True
    ) as session:
        # List to gather all archivers to run asyncronously
        tasks = []
        for dataset in datasets:
            zenodo_client = ZenodoClient(
                "dataset_doi.yaml",
                session,
                upload_key,
                publish_key,
                testing=sandbox,
            )

            tasks.append(
                archive_dataset(
                    dataset,
                    zenodo_client,
                    session,
                    initialize=initialize,
                    dry_run=dry_run,
                )
            )

        await asyncio.gather(*tasks)

"""Tool to download data resources and create archives on Zenodo for use in PUDL."""
import asyncio
import os
import pathlib

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.zenodo.api_client import ZenodoDepositionInterface


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
        connector=connector, raise_for_status=False
    ) as session:
        # List to gather all archivers to run asyncronously
        tasks = []
        for dataset in datasets:
            cls = ARCHIVERS.get(dataset)
            if not cls:
                raise RuntimeError(f"Dataset {dataset} not supported")
            else:
                downloader = cls(session)
            zenodo_deposition_interface = ZenodoDepositionInterface(
                dataset,
                downloader,
                session,
                upload_key,
                publish_key,
                deposition_settings=pathlib.Path("dataset_doi.yaml"),
                create_new=initialize,
                dry_run=dry_run,
                sandbox=sandbox,
            )

            tasks.append(zenodo_deposition_interface.run())

        results = zip(datasets, await asyncio.gather(*tasks, return_exceptions=True))
        exceptions = [
            (dataset, result)
            for dataset, result in results
            if isinstance(result, Exception)
        ]
        if exceptions:
            print(
                f"Encountered exceptions, showing traceback for last one: {[repr(e) for e in exceptions]}"
            )
            raise exceptions[-1][1]

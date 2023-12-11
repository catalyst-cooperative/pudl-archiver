"""Tool to download data resources and create archives on Zenodo for use in PUDL."""
import asyncio
import json
import logging
import os
from pathlib import Path

import aiohttp

import pudl_archiver.orchestrator  # noqa: F401
from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.orchestrator import DepositionOrchestrator

logger = logging.getLogger(f"catalystcoop.{__name__}")


def all_archivers():
    """List all Archivers that have been defined."""
    dirpath = Path(__file__).parent
    pyfiles = [
        path.relative_to(dirpath)
        for path in dirpath.glob("archivers/**/*.py")
        if path.stem != "__init__"
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
    only_years: list[int] | None = None,
    summary_file: Path | None = None,
    download_dir: str | None = None,
    auto_publish: bool = False,
):
    """A CLI for the PUDL Zenodo Storage system."""
    if sandbox:
        upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
    else:
        upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
        publish_key = os.environ["ZENODO_TOKEN_PUBLISH"]

    async def on_request_start(session, trace_config_ctx, params):
        logger.debug(f"Starting request {params.url}: headers {params.headers}")

    async def on_response_chunk_received(session, trace_config_ctx, params):
        logger.debug(f"Chunk received from {params.url}")

    async def on_request_end(session, trace_config_ctx, params):
        logger.debug(f"Ending request {params.url}: response {params.response.status}")

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_response_chunk_received.append(on_response_chunk_received)
    trace_config.on_request_end.append(on_request_end)

    connector = aiohttp.TCPConnector(limit_per_host=20, force_close=True)
    async with aiohttp.ClientSession(
        trace_configs=[trace_config], connector=connector, raise_for_status=False
    ) as session:
        # List to gather all archivers to run asyncronously
        tasks = []
        for dataset in datasets:
            cls = ARCHIVERS.get(dataset)
            if not cls:
                raise RuntimeError(f"Dataset {dataset} not supported")
            downloader = cls(session, only_years, download_directory=download_dir)
            orchestrator = DepositionOrchestrator(
                dataset,
                downloader,
                session,
                upload_key,
                publish_key,
                dataset_settings_path=Path("dataset_doi.yaml"),
                create_new=initialize,
                sandbox=sandbox,
                auto_publish=auto_publish,
            )

            tasks.append(orchestrator.run())

        results = list(
            zip(datasets, await asyncio.gather(*tasks, return_exceptions=True))
        )
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

    if summary_file is not None:
        run_summaries = [
            result.dict()
            for _, result in results
            if not isinstance(result, BaseException)
        ]

        with summary_file.open("w") as f:
            f.write(json.dumps(run_summaries, indent=2))

    # Check validation results of all runs that aren't unchanged
    validation_results = [
        result.success for _, result in results if isinstance(result, RunSummary)
    ]
    if not all(validation_results):
        raise RuntimeError("Error: archive validation tests failed.")

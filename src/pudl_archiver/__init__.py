"""Tool to download data resources and create archives on Zenodo for use in PUDL."""
import asyncio
import json
import logging
from pathlib import Path

import aiohttp

import pudl_archiver.orchestrator  # noqa: F401
from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors.zenodo.depositor import ZenodoPublishedDeposition
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings

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

    def all_subclasses(cls):
        """If a subclass has subclasses, include them in the list. Remove intermediaries."""
        subclasses = set(cls.__subclasses__())
        for c in subclasses.copy():
            subsubclasses = set(c.__subclasses__())
            if subsubclasses:
                subclasses.remove(c)
                subclasses = subclasses.union(subsubclasses)
        return subclasses

    return all_subclasses(AbstractDatasetArchiver)


ARCHIVERS = {archiver.name: archiver for archiver in all_archivers()}


async def archive_datasets(
    datasets: list[str],
    run_settings: RunSettings,
):
    """A CLI for the PUDL Zenodo Storage system."""

    async def on_request_start(session, trace_config_ctx, params):
        logger.debug(f"Starting request {params.url}: headers {params.headers}")

    async def on_response_chunk_received(session, trace_config_ctx, params):
        logger.debug(f"Chunk received from {params.url}")

    async def on_request_end(session, trace_config_ctx, params):
        logger.debug(f"Ending request {params.url}: response {params.response.status}")
        logger.debug("Sent headers: %s" % params.response.request_info.headers)

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_response_chunk_received.append(on_response_chunk_received)
    trace_config.on_request_end.append(on_request_end)

    connector = aiohttp.TCPConnector(limit_per_host=20, force_close=True)
    async with aiohttp.ClientSession(
        trace_configs=[trace_config],
        connector=connector,
        raise_for_status=False,
        timeout=aiohttp.ClientTimeout(total=10 * 60),
    ) as session:
        # List to gather all archivers to run asyncronously
        tasks = []
        for dataset in datasets:
            cls = ARCHIVERS.get(dataset)
            if not cls:
                raise RuntimeError(f"Dataset {dataset} not supported")
            downloader = cls(
                session,
                run_settings.only_years,
                download_directory=run_settings.download_dir,
            )
            depositor = await ZenodoPublishedDeposition.get_latest_version(
                dataset=dataset,
                session=session,
                settings=run_settings,
            )
            tasks.append(
                orchestrate_run(
                    dataset,
                    downloader,
                    depositor,
                    session,
                )
            )

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

    if run_settings.summary_file is not None:
        run_summaries = [
            result.dict()
            for _, [result, published] in results
            if not isinstance(result, BaseException)
        ]

        with run_settings.summary_file.open("w") as f:
            f.write(json.dumps(run_summaries, indent=2))

    # Check validation results of all runs that aren't unchanged
    validation_results = [
        result.success
        for _, [result, published] in results
        if isinstance(result, RunSummary)
    ]
    if not all(validation_results):
        raise RuntimeError("Error: archive validation tests failed.")

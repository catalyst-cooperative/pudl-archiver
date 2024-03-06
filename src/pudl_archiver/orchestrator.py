"""Core routines for archiving raw data packages."""
import io
import logging
import re

import aiohttp

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors.depositor import Depositor
from pudl_archiver.frictionless import DataPackage

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _datapackage_worth_changing(
    old_datapackage: DataPackage | None, new_datapackage: DataPackage
) -> bool:
    # ignore differences in created/version
    # ignore differences resource paths if it's just some ID number changing...
    if old_datapackage is None:
        return True
    for field in new_datapackage.model_dump():
        if field in {"created", "version"}:
            continue
        if field == "resources":
            for r in old_datapackage.resources + new_datapackage.resources:
                r.path = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.path))
                r.remote_url = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.remote_url))
        if getattr(new_datapackage, field) != getattr(old_datapackage, field):
            return True
    return False


async def orchestrate_run(
    dataset: str,
    downloader: AbstractDatasetArchiver,
    depositor: Depositor,
    session: aiohttp.ClientSession,
) -> RunSummary:
    """Use downloader and depositor to archive a dataset."""
    resources = {}
    original_datapackage = await depositor.get_file("datapackage.json")
    async with depositor.open_draft() as draft:
        async for name, resource in downloader.download_all_resources([]):
            resources[name] = resource
            await draft.add_resource(name, resource)

        # Delete files in draft that weren't downloaded by downloader
        for filename in await draft.list_files():
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                draft.delete_file(filename)

        # Create new datapackage
        new_datapackage = draft.generate_datapackage(resources)

        # Add datapackage if it's changed
        if _datapackage_worth_changing(original_datapackage, new_datapackage):
            datapackage_json = io.BytesIO(
                bytes(
                    new_datapackage.model_dump_json(by_alias=True, indent=4),
                    encoding="utf-8",
                )
            )
            await draft.create_file("datapackage.json", datapackage_json)

        # Validate run
        validations = downloader.validate_dataset(
            original_datapackage, new_datapackage, resources
        )
        summary = RunSummary(
            dataset,
            original_datapackage,
            new_datapackage,
            validations,
            draft.get_deposition_link(),
        )
        draft.validate_run(summary)
        return summary

"""Core routines for archiving raw data packages."""
import io
import logging
import re
from pathlib import Path

import aiohttp
from pydantic import BaseModel, ConfigDict

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.depositors.depositor import DepositionAction, DepositionChange
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import Url

logger = logging.getLogger(f"catalystcoop.{__name__}")


class _UploadSpec(BaseModel):
    """Defines an upload that will be done by ZenodoDepositionInterface."""

    source: io.IOBase | Path
    dest: str
    model_config = ConfigDict(arbitrary_types_allowed=True)


class FileWrapper(io.BytesIO):
    """Minimal wrapper arount BytesIO to override close method to work around aiohttp."""

    def __init__(self, content: bytes):
        """Call base class __init__."""
        super().__init__(content)

    def close(self):
        """Don't close file, so aiohttp can't unexpectedly close files."""
        pass

    def actually_close(self):
        """Actually close the file for internal use."""
        super().close()


class DepositionOrchestrator:
    """Interface to a single Deposition."""

    def __init__(
        self,
        data_source_id: str,
        downloader: AbstractDatasetArchiver,
        session: aiohttp.ClientSession,
        dataset_settings_path: Path,
        create_new: bool = False,
        dry_run: bool = True,
        sandbox: bool = True,
        auto_publish: bool = False,
        refresh_metadata: bool = False,
        resume_run: bool = False,
    ):
        """Prepare the ZenodoStorage interface.

        Args:
            data_source_id: Data source ID.
            downloader: AbstractDatasetArchiver that actually handles the data
                downloads.
            session: Async http client session manager.
                DOIs are stored.
            dataset_settings_path: Path to settings file for each dataset.
            create_new: whether or not we are adding a new dataset.
            dry_run: Whether or not we upload files to Zenodo in this run.
            sandbox: Whether or not we are in a sandbox environment
            auto_publish: Whether we automatically publish the draft when we're
                done, vs. letting a human approve of it.
            refresh_metadata: Regenerate metadata from PUDL data source rather than
                existing archived metadata.
            resume_run: Attempt to resume a run that was previously interrupted.

        Returns:
            DepositionOrchestrator
        """
        if create_new and resume_run:
            raise RuntimeError(
                "create_new and resume_run should not both be set."
                "create new will be deduced from checkpoint when resume_run is set."
            )
        self.sandbox = sandbox
        self.data_source_id = data_source_id

        # TODO (daz): pass in the depositor separately too - no reason to couple this class to Zenodo
        self.session = session

        self.auto_publish = auto_publish
        self.refresh_metadata = refresh_metadata
        self.resume_run = resume_run

        self.depositor = ZenodoDepositor(
            data_source_id, session, dataset_settings_path, self.sandbox
        )
        self.downloader = downloader

        self.dry_run = dry_run

        self.create_new = create_new

        self.changes: list[DepositionChange] = []

    async def run(self) -> RunSummary:
        """Run the entire deposition update process.

        1. Create pending deposition version to stage changes.
        2. Download resources.
        3. Update pending deposition version.
        4. If there were updates, publish.
        5. Update the dataset settings if this was a new deposition.

        Returns:
            RunSummary object.
        """
        self.changes = []
        existing_resources = await self.depositor.prepare_depositor(
            self.create_new, self.resume_run, self.refresh_metadata
        )
        resources = await self._download_then_upload_resources(
            self.downloader,
            existing_resources,
        )
        for deletion in self._get_deletions(resources):
            await self._apply_change(deletion)

        old_datapackage, new_datapackage = await self._update_datapackage(
            resources=resources
        )
        run_summary = self._summarize_run(
            old_datapackage,
            new_datapackage,
            resources,
            self.depositor.get_deposition_link(),
        )

        if not run_summary.success:
            logger.error(
                "Archive validation failed. Not publishing new archive, kept "
                f"draft at {self.depositor.get_deposition_link()} for inspection."
            )
            return run_summary

        await self._publish()
        return run_summary

    def _summarize_run(
        self,
        old_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        resources: dict[str, ResourceInfo],
        draft_url: Url,
    ) -> RunSummary:
        validations = self.downloader.validate_dataset(
            old_datapackage, new_datapackage, resources
        )

        return RunSummary.create_summary(
            self.data_source_id,
            old_datapackage,
            new_datapackage,
            validations,
            record_url=draft_url,
        )

    async def _download_then_upload_resources(
        self,
        downloader: AbstractDatasetArchiver,
        existing_resources: dict[str, ResourceInfo],
    ) -> dict[str, ResourceInfo]:
        resources = existing_resources
        async for name, resource in downloader.download_all_resources(
            list(resources.keys())
        ):
            resources[name] = resource
            change = self.depositor.generate_change(name, resource)
            # Leave immediately after generating changes if dry_run
            if self.dry_run:
                continue
            if change:
                await self._apply_change(change)

        return resources

    def _get_deletions(
        self, resources: dict[str, ResourceInfo]
    ) -> list[DepositionChange]:
        # Delete files not included in new deposition
        files_to_delete = []
        for filename in self.depositor.get_existing_files():
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                files_to_delete.append(
                    DepositionChange(DepositionAction.DELETE, name=filename)
                )

        return files_to_delete

    async def _apply_change(self, change: DepositionChange) -> None:
        """Actually upload and delete what we listed in self.uploads/deletes.

        Args:
            draft: the draft to make these changes to
            change: the change to make
        """
        self.changes.append(change)
        if self.dry_run:
            logger.info(f"Dry run, skipping {change}")
            return
        if change.action_type in [DepositionAction.DELETE, DepositionAction.UPDATE]:
            await self.depositor.delete_file(change.name)
        if change.action_type in [DepositionAction.CREATE, DepositionAction.UPDATE]:
            if change.resource is None:
                raise RuntimeError("Must pass a resource to be uploaded.")

            await self._upload_file(
                _UploadSpec(source=change.resource, dest=change.name)
            )

    async def _upload_file(self, upload: _UploadSpec):
        if isinstance(upload.source, io.IOBase):
            wrapped_file = FileWrapper(upload.source.read())
        else:
            with upload.source.open("rb") as f:
                wrapped_file = FileWrapper(f.read())

        await self.depositor.create_file(upload.dest, wrapped_file)

        wrapped_file.actually_close()

    async def _update_datapackage(
        self,
        resources: dict[str, ResourceInfo],
    ) -> tuple[DataPackage, DataPackage | None]:
        """Get new datapackage and check if it's worth uploading."""
        new_datapackage, old_datapackage = self.depositor.update_datapackage(resources)
        if old_datapackage is None:
            action = DepositionAction.CREATE
        else:
            action = DepositionAction.UPDATE

        if self._datapackage_worth_changing(old_datapackage, new_datapackage):
            datapackage_json = io.BytesIO(
                bytes(
                    new_datapackage.model_dump_json(by_alias=True, indent=4),
                    encoding="utf-8",
                )
            )
            await self._apply_change(
                DepositionChange(
                    action_type=action,
                    name="datapackage.json",
                    resource=datapackage_json,
                ),
            )
        return new_datapackage, old_datapackage

    def _datapackage_worth_changing(
        self, old_datapackage: DataPackage | None, new_datapackage: DataPackage
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

    async def _publish(self) -> None:
        if self.dry_run:
            logger.info("Dry run - not publishing at all.")
            return
        if self.auto_publish:
            await self.depositor.publish_deposition()
        else:
            logger.info("Skipping publishing deposition to allow manual review.")
            logger.info(
                f"Review new deposition at {self.depositor.get_deposition_link()}"
            )

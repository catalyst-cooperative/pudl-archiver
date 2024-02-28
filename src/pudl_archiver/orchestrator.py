"""Core routines for archiving raw data packages."""
import io
import logging
import re
from dataclasses import dataclass
from enum import Enum, auto
from hashlib import md5
from pathlib import Path

import aiohttp
import yaml
from pydantic import BaseModel, ConfigDict

from pudl_archiver import checkpoints
from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import Url, retry_async
from pudl_archiver.zenodo.entities import (
    Deposition,
    DepositionFile,
    DepositionMetadata,
    Doi,
    SandboxDoi,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class _DepositionAction(Enum):
    CREATE = (auto(),)
    UPDATE = (auto(),)
    DELETE = (auto(),)


@dataclass
class _DepositionChange:
    action_type: _DepositionAction
    name: str
    resource: io.IOBase | Path | None = None


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


class DatasetSettings(BaseModel):
    """Simple model to validate doi's in settings."""

    production_doi: Doi | None = None
    sandbox_doi: SandboxDoi | None = None


def _compute_md5(file_path: Path) -> str:
    """Compute an md5 checksum to compare to files in zenodo deposition."""
    hash_md5 = md5()  # noqa: S324
    with Path.open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


class DepositionOrchestrator:
    """Interface to a single Deposition."""

    def __init__(
        self,
        data_source_id: str,
        downloader: AbstractDatasetArchiver,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
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
            upload_key: Zenodo API upload key.
            publish_key: Zenodo API publish key.
            dataset_settings_path: where the various production/sandbox concept
                DOIs are stored.
            create_new: whether or not we are initializing a new Zenodo Concept DOI.
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

        self.upload_key = upload_key
        self.publish_key = publish_key

        self.auto_publish = auto_publish
        self.refresh_metadata = refresh_metadata
        self.resume_run = resume_run

        self.depositor = ZenodoDepositor(upload_key, publish_key, session, self.sandbox)
        self.downloader = downloader

        self.dry_run = dry_run

        self.create_new = create_new
        self.dataset_settings_path = dataset_settings_path
        with Path.open(dataset_settings_path) as f:
            self.dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }

        self.changes: list[_DepositionChange] = []

    async def _create_new_deposition(self) -> Deposition:
        metadata = DepositionMetadata.from_data_source(self.data_source_id)
        if not metadata.keywords:
            raise AssertionError(
                "New dataset is missing keywords and cannot be archived."
            )
        return await self.depositor.create_deposition(metadata)

    async def _get_existing_deposition(
        self, dataset_settings: dict[str, DatasetSettings], data_source_id: str
    ) -> Deposition:
        settings = dataset_settings[data_source_id]
        doi = settings.sandbox_doi if self.sandbox else settings.production_doi
        if not doi:
            raise RuntimeError("Must pass a valid DOI if create_new is False")
        return await self.depositor.get_deposition(doi)

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
        if self.resume_run:
            run_history = checkpoints.load_checkpoint(self.data_source_id)
            original = run_history.deposition
            draft = original
            self.create_new = run_history.create_new
            existing_resources = run_history.resources
        else:
            existing_resources = {}
            if self.create_new:
                original = await self._create_new_deposition()
                draft = original
            else:
                original = await self._get_existing_deposition(
                    self.dataset_settings, self.data_source_id
                )
                draft = await self.depositor.get_new_version(
                    original,
                    clobber=True,
                    data_source_id=self.data_source_id,
                    refresh_metadata=self.refresh_metadata,
                )

        draft = await self.depositor.get_deposition_by_id(draft.id_)
        resources = await self._download_then_upload_resources(
            draft,
            self.downloader,
            existing_resources,
        )
        for deletion in self._get_deletions(draft, resources):
            await self._apply_change(draft, deletion)

        draft = await self.depositor.get_deposition_by_id(draft.id_)
        new_datapackage, old_datapackage = await self._update_datapackage(
            original=original, draft=draft, resources=resources
        )
        run_summary = self._summarize_run(
            old_datapackage, new_datapackage, resources, draft.links.html
        )

        if len(run_summary.file_changes) == 0 and not self._datapackage_worth_changing(
            old_datapackage, new_datapackage
        ):
            logger.info(
                f"No changes detected, kept draft at {draft.links.html} for "
                "inspection."
            )
            return run_summary

        if not run_summary.success:
            logger.error(
                "Archive validation failed. Not publishing new archive, kept "
                f"draft at {draft.links.html} for inspection."
            )
            return run_summary

        await self._publish(draft)
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
        draft: Deposition,
        downloader: AbstractDatasetArchiver,
        existing_resources: dict[str, ResourceInfo],
    ) -> dict[str, ResourceInfo]:
        resources = existing_resources
        async for name, resource in downloader.download_all_resources(
            list(resources.keys())
        ):
            resources[name] = resource
            change = self._generate_change(name, resource, draft.files_map)
            # Leave immediately after generating changes if dry_run
            if self.dry_run:
                continue
            if change:
                await self._apply_change(draft, change)
            else:
                logger.info(f"No changes detected for {resource.local_path}")
            checkpoints.save_checkpoint(
                self.data_source_id, draft, resources, self.create_new
            )
        return resources

    def _generate_change(
        self, name: str, resource: ResourceInfo, files: dict[str, DepositionFile]
    ) -> _DepositionChange | None:
        action = None
        if name not in files:
            logger.info(f"Adding {name} to deposition.")

            action = _DepositionAction.CREATE
        else:
            file_info = files[name]

            # If file is not exact match for existing file, update with new file
            if (local_md5 := _compute_md5(resource.local_path)) != file_info.checksum:
                logger.info(
                    f"Updating {name}: local hash {local_md5} vs. remote {file_info.checksum}"
                )
                action = _DepositionAction.UPDATE

        if action is None:
            return None

        return _DepositionChange(
            action_type=action,
            name=name,
            resource=resource.local_path,
        )

    def _get_deletions(
        self, draft: Deposition, resources: dict[str, ResourceInfo]
    ) -> list[_DepositionChange]:
        # Delete files not included in new deposition
        files_to_delete = []
        for filename in draft.files_map:
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                files_to_delete.append(
                    _DepositionChange(_DepositionAction.DELETE, name=filename)
                )

        return files_to_delete

    async def _apply_change(self, draft: Deposition, change: _DepositionChange) -> None:
        """Actually upload and delete what we listed in self.uploads/deletes.

        Args:
            draft: the draft to make these changes to
            change: the change to make
        """
        self.changes.append(change)
        if self.dry_run:
            logger.info(f"Dry run, skipping {change}")
            return
        if change.action_type in [_DepositionAction.DELETE, _DepositionAction.UPDATE]:
            file_info = draft.files_map[change.name]
            await self.depositor.delete_file(draft, file_info.filename)
        if change.action_type in [_DepositionAction.CREATE, _DepositionAction.UPDATE]:
            if change.resource is None:
                raise RuntimeError("Must pass a resource to be uploaded.")

            await self._upload_file(
                draft, _UploadSpec(source=change.resource, dest=change.name)
            )

    async def _upload_file(self, draft: Deposition, upload: _UploadSpec):
        if isinstance(upload.source, io.IOBase):
            wrapped_file = FileWrapper(upload.source.read())
        else:
            with upload.source.open("rb") as f:
                wrapped_file = FileWrapper(f.read())

        await self.depositor.create_file(draft, upload.dest, wrapped_file)

        wrapped_file.actually_close()

    def _update_dataset_settings(self, published_deposition):
        # Get new DOI and update settings
        # TODO (daz): split this IO out too.
        if self.sandbox:
            sandbox_doi = published_deposition.conceptdoi
            production_doi = self.dataset_settings.get(
                self.data_source_id, DatasetSettings()
            ).production_doi
        else:
            production_doi = published_deposition.conceptdoi
            sandbox_doi = self.dataset_settings.get(
                self.data_source_id, DatasetSettings()
            ).sandbox_doi

        self.dataset_settings[self.data_source_id] = DatasetSettings(
            sandbox_doi=sandbox_doi, production_doi=production_doi
        )

        # Update doi settings YAML
        with Path.open(self.dataset_settings_path, "w") as f:
            raw_settings = {
                name: settings.dict()
                for name, settings in self.dataset_settings.items()
            }
            yaml.dump(raw_settings, f)

    async def _update_datapackage(
        self,
        original: Deposition,
        draft: Deposition,
        resources: dict[str, ResourceInfo],
    ) -> tuple[DataPackage, DataPackage | None]:
        """Create new frictionless datapackage for deposition.

        Args:
            draft: the draft we're trying to describe
            resources: Dictionary mapping resources to ResourceInfo which is used to
            generate new datapackage - we need this for the partition information.

        Returns:
            new DataPackage, old DataPackage
        """
        logger.info(f"Creating new datapackage.json for {self.data_source_id}")
        old_datapackage = None
        if "datapackage.json" in original.files_map:
            url = original.files_map["datapackage.json"].links.canonical
            response = await self.depositor.request(
                "GET",
                url,
                "Download old datapackage",
                parse_json=False,
                headers=self.depositor.auth_write,
            )
            response_bytes = await retry_async(response.read)
            old_datapackage = DataPackage.model_validate_json(response_bytes)

        # Create updated datapackage
        datapackage = DataPackage.from_filelist(
            self.data_source_id,
            [f for f in draft.files if f.filename != "datapackage.json"],
            resources,
            draft.metadata.version,
        )

        datapackage_json = io.BytesIO(
            bytes(
                datapackage.model_dump_json(by_alias=True, indent=4), encoding="utf-8"
            )
        )

        if "datapackage.json" in draft.files_map:
            action = _DepositionAction.UPDATE
        else:
            action = _DepositionAction.CREATE

        if self._datapackage_worth_changing(old_datapackage, datapackage):
            await self._apply_change(
                draft,
                _DepositionChange(
                    action_type=action,
                    name="datapackage.json",
                    resource=datapackage_json,
                ),
            )

        return datapackage, old_datapackage

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

    async def _publish(self, draft: Deposition) -> None:
        if self.dry_run:
            logger.info("Dry run - not publishing at all.")
            return
        if self.auto_publish:
            published = await self.depositor.publish_deposition(draft)
            if self.create_new:
                self._update_dataset_settings(published)
        else:
            logger.info("Skipping publishing deposition to allow manual review.")
            logger.info(f"Review new deposition at {draft.links.html}")

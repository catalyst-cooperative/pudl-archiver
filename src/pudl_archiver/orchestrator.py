"""Core routines for archiving raw data packages."""
import io
import logging
from enum import Enum, auto
from hashlib import md5
from pathlib import Path

import aiohttp
import yaml
from pydantic import BaseModel

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary, Unchanged
from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import retry_async
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


class _UploadSpec(BaseModel):
    """Defines an upload that will be done by ZenodoDepositionInterface."""

    source: io.IOBase | Path
    dest: str

    class Config:
        arbitrary_types_allowed = True


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
    hash_md5 = md5()  # nosec: B324
    with open(file_path, "rb") as f:
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
        deposition_settings: Path,
        create_new: bool = False,
        dry_run: bool = True,
        sandbox: bool = True,
    ):
        """Prepare the ZenodoStorage interface.

        Args:
            data_source_id: Data source ID.
            session: Async http client session manager.
            upload_key: Zenodo API upload key.
            publish_key: Zenodo API publish key.

        Returns:
            DepositionOrchestrator
        """
        if sandbox:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

        self.sandbox = sandbox
        self.data_source_id = data_source_id

        # TODO (daz): pass in the depositor separately too - no reason to couple this class to Zenodo
        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        self.depositor = ZenodoDepositor(upload_key, publish_key, session, self.sandbox)
        self.downloader = downloader

        # TODO (daz): don't hold references to the depositions at the instance level.
        self.deposition: Deposition | None = None
        self.new_deposition: Deposition | None = None
        self.deposition_files: dict[str, DepositionFile] = {}

        self.dry_run = dry_run

        self.create_new = create_new
        self.deposition_settings = deposition_settings
        with open(deposition_settings) as f:
            self.dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }

    async def _initialize(self):
        if self.create_new:
            doi = None
            self.deposition = await self._create_deposition(self.data_source_id)
        else:
            settings = self.dataset_settings[self.data_source_id]
            doi = settings.sandbox_doi if self.sandbox else settings.production_doi
            if not doi:
                raise RuntimeError("Must pass a valid DOI if create_new is False")

            self.deposition = await self.depositor.get_deposition(doi)
        self.new_deposition = await self.depositor.get_new_version(
            self.deposition, clobber=not self.create_new
        )

        # TODO (daz): stop using self.deposition_files, use the files lists on the depositions
        # Map file name to file metadata for all files in deposition
        self.deposition_files = await self._remote_fileinfo(self.new_deposition)

    # TODO (daz): inline this.
    async def _remote_fileinfo(self, deposition: Deposition):
        """Return info on all files contained in deposition.

        Args:
            deposition: Deposition for which to return file info.

        Returns:
            Dictionary mapping filenames to DepositionFile metadata objects.
        """
        return {f.filename: f for f in deposition.files}

    # TODO (daz): inline this.
    async def _create_deposition(self, data_source_id: str) -> Deposition:
        """Create a Zenodo deposition resource.

        This should only be called once for a given data source.  The deposition will be
        prepared in draft form, so that files can be added prior to publication.

        Args:
            data_source_id: Data source ID that will be used to generate zenodo metadata
            from data source metadata.

        Returns:
            Deposition object, per
            https://developers.zenodo.org/?python#depositions
        """
        metadata = DepositionMetadata.from_data_source(data_source_id)
        return await self.depositor.create_deposition(metadata)

    async def run(self) -> RunSummary | Unchanged:
        """Run the entire deposition update process.

        1. Create pending deposition version to stage changes.
        2. Download resources.
        3. Update pending deposition version.
        4. If there were updates, publish.
        5. Update the dataset settings if this was a new deposition.

        Returns:
            RunSummary object or Unchanged if no changes are detected or run is a dry run.
        """
        await self._initialize()

        resources = {}
        changed = False
        async for name, resource in self.downloader.download_all_resources():
            resources[name] = resource
            action = self._generate_changes(name, resource)

            # Leave immediately after generating changes if dry_run
            if self.dry_run:
                continue

            if action:
                changed = True
                await self._apply_changes(action, name, resource)

        # Check for files that should no longer be in deposition
        files_to_delete = self._get_files_to_delete(resources)
        changed = changed or len(files_to_delete) > 0
        if self.dry_run:
            logger.info("Dry run, aborting")
            return Unchanged(dataset_name=self.data_source_id, reason="Dry run.")

        # Delete files no longer in deposition
        [
            await self._apply_changes(_DepositionAction.DELETE, name)
            for name in files_to_delete
        ]

        self.new_deposition = await self.depositor.get_record(self.new_deposition.id_)
        if changed:
            # If there are any changes detected update datapackage and publish
            new_datapackage, old_datapackage = await self._update_datapackage(resources)

            run_summary = self.downloader.generate_summary(
                old_datapackage, new_datapackage, resources
            )
            if not run_summary.success:
                logger.error("Archive validation failed. Not publishing new archive.")
                await self.depositor.delete_deposition(self.new_deposition)
                return run_summary

            published = await self.depositor.publish_deposition(self.new_deposition)
            if self.create_new:
                self._update_dataset_settings(published)
            return run_summary
        else:
            logger.info("No changes detected.")
            await self.depositor.delete_deposition(self.new_deposition)
            return Unchanged(dataset_name=self.data_source_id)

    def _generate_changes(self, name: str, resource: ResourceInfo) -> _DepositionAction:
        if name not in self.deposition_files:
            logger.info(f"Adding {name} to deposition.")

            return _DepositionAction.CREATE
        else:
            file_info = self.deposition_files[name]

            # If file is not exact match for existing file, update with new file
            if (
                not (local_md5 := _compute_md5(resource.local_path))
                == file_info.checksum
            ):
                logger.info(
                    f"Updating {name}: local hash {local_md5} vs. remote {file_info.checksum}"
                )
                return _DepositionAction.UPDATE

        return None

    def _get_files_to_delete(self, resources) -> list[str]:
        # Delete files not included in new deposition
        files_to_delete = []
        logger.info(f"Depo files: {list(self.deposition_files.keys())}")
        for filename, file_info in self.deposition_files.items():
            if filename not in resources and filename != "datapackage.json":
                logger.info(f"Deleting {filename} from deposition.")
                files_to_delete.append(filename)

        return files_to_delete

    async def _apply_changes(
        self, action: _DepositionAction, name: str, resource: ResourceInfo | None = None
    ):
        """Actually upload and delete what we listed in self.uploads/deletes."""
        match action:
            case _DepositionAction.DELETE:
                file_info = self.deposition_files[name]
                await self.depositor.delete_file(
                    self.new_deposition, file_info.filename
                )
            case _DepositionAction.CREATE:
                if resource is None:
                    raise RuntimeError("Must pass a resource to be uploaded.")

                await self._upload_file(
                    _UploadSpec(source=resource.local_path, dest=name)
                )
            case _DepositionAction.UPDATE:
                if resource is None:
                    raise RuntimeError("Must pass a resource to be uploaded.")

                file_info = self.deposition_files[name]
                await self.depositor.delete_file(
                    self.new_deposition, file_info.filename
                )
                await self._upload_file(
                    _UploadSpec(source=resource.local_path, dest=name)
                )

    async def _upload_file(self, upload: _UploadSpec):
        if isinstance(upload.source, io.IOBase):
            wrapped_file = FileWrapper(upload.source.read())
        else:
            with upload.source.open("rb") as f:
                wrapped_file = FileWrapper(f.read())

        await self.depositor.create_file(self.new_deposition, upload.dest, wrapped_file)

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
        with open(self.deposition_settings, "w") as f:
            raw_settings = {
                name: settings.dict()
                for name, settings in self.dataset_settings.items()
            }
            yaml.dump(raw_settings, f)

    async def _update_datapackage(self, resources: dict[str, ResourceInfo]):
        """Create new frictionless datapackage for deposition.

        Args:
            resources: Dictionary mapping resources to ResourceInfo which is used to
            generate new datapackage
        Returns:
            Updated Deposition.
        """
        if self.new_deposition is None:
            return None, None

        logger.info(f"Creating new datapackage.json for {self.data_source_id}")
        files = {file.filename: file for file in self.new_deposition.files}

        old_datapackage = None
        if "datapackage.json" in files:
            # Download old datapackage
            url = files["datapackage.json"].links.download
            response = await self.depositor.request(
                "GET",
                url,
                "Download old datapackage",
                parse_json=False,
                headers=self.depositor.auth_write,
            )
            response_bytes = await retry_async(response.read)
            old_datapackage = DataPackage.parse_raw(response_bytes)

            # Stage old datapackge to be deleted
            await self._apply_changes(_DepositionAction.DELETE, "datapackage.json")
            files.pop("datapackage.json")

        # Create updated datapackage
        datapackage = DataPackage.from_filelist(
            self.data_source_id,
            files.values(),
            resources,
            self.new_deposition.metadata.version,
        )

        datapackage_json = io.BytesIO(
            bytes(datapackage.json(by_alias=True), encoding="utf-8")
        )

        await self._upload_file(
            _UploadSpec(source=datapackage_json, dest="datapackage.json")
        )

        return datapackage, old_datapackage

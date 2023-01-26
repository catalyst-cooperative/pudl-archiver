"""Core routines for archiving raw data packages on Zenodo."""
import io
import logging
from hashlib import md5
from pathlib import Path

import aiohttp
import yaml
from pydantic import BaseModel

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.zenodo.entities import (
    Deposition,
    DepositionFile,
    DepositionMetadata,
    Doi,
    SandboxDoi,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


class _UploadSpec(BaseModel):
    """Defines an upload that will be done by ZenodoDepositionInterface."""

    source: io.IOBase | Path
    dest: str

    class Config:
        arbitrary_types_allowed = True


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


class ZenodoDepositionInterface:
    """Interface to a single Zenodo Deposition."""

    def __init__(
        self,
        data_source_id: str,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        dataset_settings: dict[str, DatasetSettings],
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
            ZenodoDepositionInterface
        """
        if sandbox:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

        self.sandbox = sandbox
        self.data_source_id = data_source_id

        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        self.depositor = ZenodoDepositor(upload_key, publish_key, session, self.sandbox)

        # Map resource name to resource partitions
        self.resource_parts: dict[str, dict] = {}

        self.deposition: Deposition | None = None
        self.new_deposition: Deposition | None = None
        self.deposition_files: dict[str, DepositionFile] = {}

        self.dry_run = dry_run
        # We accumulate changes in a changeset, then apply - makes dry runs and testing easier.
        # upload takes (source: IOBase | Path, dest: str) tuples; delete just takes the str key to delete.
        self.uploads: list[_UploadSpec] = []
        self.deletes: list[DepositionFile] = []
        self.changed = False

        self.create_new = create_new
        self.dataset_settings = dataset_settings
        self.deposition_settings_path = deposition_settings

    @classmethod
    async def open_interface(
        cls,
        data_source_id: str,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        deposition_settings: Path,
        doi: str | None = None,
        create_new: bool = False,
        dry_run: bool = True,
        sandbox: bool = True,
    ):
        """Create deposition interface to existing zenodo deposition.

        Args:
            data_source_id: Data source ID.
            session: Async http client session manager.
            upload_key: Zenodo API upload key.
            publish_key: Zenodo API publish key.
            doi: Concept DOI pointing to deposition. If none, create_new should be true.
            create_new: Create new zenodo deposition.

        Returns:
            ZenodoDepositionInterface
        """
        with open(deposition_settings) as f:
            dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }
        interface = cls(
            data_source_id,
            session,
            upload_key,
            publish_key,
            dataset_settings,
            deposition_settings,
            create_new=create_new,
            dry_run=dry_run,
            sandbox=sandbox,
        )

        if create_new:
            doi = None
            interface.deposition = await interface.create_deposition(data_source_id)
        else:
            settings = dataset_settings[data_source_id]
            doi = settings.sandbox_doi if sandbox else settings.production_doi
            if not doi:
                raise RuntimeError("Must pass a valid DOI if create_new is False")

            interface.deposition = await interface.depositor.get_deposition(doi)

        interface.new_deposition = await interface.depositor.get_new_version(
            interface.deposition
        )

        # Map file name to file metadata for all files in deposition
        interface.deposition_files = await interface.remote_fileinfo(
            interface.new_deposition
        )

        return interface

    async def remote_fileinfo(self, deposition: Deposition):
        """Return info on all files contained in deposition.

        Args:
            deposition: Deposition for which to return file info.

        Returns:
            Dictionary mapping filenames to DepositionFile metadata objects.
        """
        return {f.filename: f for f in deposition.files}

    async def create_deposition(self, data_source_id: str) -> Deposition:
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

    async def add_files(self, resources: dict[str, ResourceInfo]):
        """Add all downloaded files to zenodo deposition.

        The new Zenodo deposition (self.new_deposition) contains all of the files
        from the previous version. This function will loop through all downloaded
        files and compare them by name and checksum to the remote files already
        contained in the deposition. It will then upload new files, update files that
        already exist, and delete any files that are no longer contained in the new
        deposition.

        Args:
            resources: Dictionary mapping filenames to ResourceInfo objects,
            which contain local path to resource and working partitions.

        Returns:
            Updated Deposition object.
        """
        self._generate_changes(resources)
        await self._apply_changes()
        self.new_deposition = await self.depositor.get_record(self.new_deposition.id_)
        await self.update_datapackage(resources)
        await self._apply_changes()

        if self.changed:
            published = await self.depositor.publish_deposition(self.new_deposition)
            if self.create_new:
                self._update_dataset_settings(published)

    def _generate_changes(self, resources):
        for name, resource in resources.items():
            filepath = resource.local_path
            if name not in self.deposition_files:
                logger.info(f"Adding {name} to deposition.")

                self.uploads.append(_UploadSpec(source=filepath, dest=filepath.name))
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
                    self.deletes.append(file_info)
                    self.uploads.append(
                        _UploadSpec(source=filepath, dest=filepath.name)
                    )

        # Delete files not included in new deposition
        for filename, file_info in self.deposition_files.items():
            if filename not in resources and filename != "datapackage.json":
                self.deletes.append(file_info)

        self.changed = len(self.uploads or self.deletes) > 0

    async def _apply_changes(self):
        """Actually upload and delete what we listed in self.uploads/deletes."""
        logger.info(f"To delete: {self.deletes}")
        logger.info(f"To upload: {self.uploads}")
        if self.dry_run:
            logger.info("Dry run, aborting.")
            return
        for file_info in self.deletes:
            await self.depositor.delete_file(self.new_deposition, file_info.filename)
        self.deletes = []
        for upload in self.uploads:
            if isinstance(upload.source, io.IOBase):
                await self.depositor.create_file(
                    self.new_deposition, upload.dest, upload.source
                )
            else:
                with upload.source.open("rb") as f:
                    await self.depositor.create_file(
                        self.new_deposition, upload.dest, f
                    )
        self.uploads = []

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
        with open(self.deposition_settings_path, "w") as f:
            raw_settings = {
                name: settings.dict()
                for name, settings in self.dataset_settings.items()
            }
            yaml.dump(raw_settings, f)

    async def update_datapackage(self, resources: dict[str, ResourceInfo]):
        """Create new frictionless datapackage for deposition.

        Args:
            resources: Dictionary mapping resources to ResourceInfo which is used to
            generate new datapackage
        Returns:
            Updated Deposition.
        """
        # If nothing has changed, don't add new datapackage
        if not self.changed:
            return None

        if self.new_deposition is None:
            return

        logger.info(f"Creating new datapackage.json for {self.data_source_id}")
        files = {file.filename: file for file in self.new_deposition.files}

        if "datapackage.json" in files:
            self.deletes.append(files["datapackage.json"])
            files.pop("datapackage.json")

        datapackage = DataPackage.from_filelist(
            self.data_source_id, files.values(), resources
        )

        datapackage_json = io.BytesIO(
            bytes(datapackage.json(by_alias=True), encoding="utf-8")
        )

        self.uploads.append(
            _UploadSpec(source=datapackage_json, dest="datapackage.json")
        )

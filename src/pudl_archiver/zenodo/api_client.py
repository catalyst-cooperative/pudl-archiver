"""Core routines for archiving raw data packages on Zenodo."""
import io
import json
import logging
from contextlib import asynccontextmanager
from hashlib import md5
from pathlib import Path
from typing import BinaryIO

import aiohttp
import semantic_version
import yaml
from pydantic import BaseModel

from pudl_archiver.archivers.classes import ResourceInfo
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
        api_root: str,
        dry_run: bool = True,
    ):
        """Prepare the ZenodoStorage interface.

        Args:
            data_source_id: Data source ID.
            session: Async http client session manager.
            upload_key: Zenodo API upload key.
            publish_key: Zenodo API publish key.
            api_root: Zenodo API root URL. Points to sandbox or production API.

        Returns:
            ZenodoDepositionInterface

        """
        self.data_source_id = data_source_id

        self.api_root = api_root
        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        # Map resource name to resource partitions
        self.resource_parts: dict[str, dict] = {}

        self.deposition: Deposition = None
        self.new_deposition: Deposition = None
        self.deposition_files: dict[str, DepositionFile] = {}

        self.dry_run = dry_run
        # We accumulate changes in a changeset, then apply - makes dry runs and testing easier.
        # upload takes (source: IOBase | Path, dest: str) tuples; delete just takes the str key to delete.
        self.uploads: list[_UploadSpec] = []
        self.deletes: list[str] = []

    @classmethod
    async def open_interface(
        cls,
        data_source_id: str,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        api_root: str,
        doi: str | None = None,
        create_new: bool = False,
        dry_run: bool = True,
    ):
        """Create deposition interface to existing zenodo deposition.

        Args:
            data_source_id: Data source ID.
            session: Async http client session manager.
            upload_key: Zenodo API upload key.
            publish_key: Zenodo API publish key.
            api_root: Zenodo API root URL. Points to sandbox or production API.
            doi: Concept DOI pointing to deposition. If none, create_new should be true.
            create_new: Create new zenodo deposition.

        Returns:
            ZenodoDepositionInterface

        """
        interface = cls(
            data_source_id, session, upload_key, publish_key, api_root, dry_run=dry_run
        )

        if create_new:
            interface.deposition = await interface.create_deposition(data_source_id)
        else:
            if not doi:
                raise RuntimeError("Must pass a valid DOI if create_new is False")

            interface.deposition = await interface.get_deposition(doi)

        interface.new_deposition = await interface.new_deposition_version()

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
        url = deposition.links.files
        params = {"access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            raw_json = await response.json()

        return {file["filename"]: DepositionFile(**file) for file in raw_json}

    async def create_deposition(self, data_source_id: str):
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
        url = f"{self.api_root}/deposit/depositions"
        params = {"access_token": self.upload_key}
        headers = {"Content-Type": "application/json"}

        metadata = DepositionMetadata.from_data_source(data_source_id)
        data = json.dumps(
            {
                "metadata": metadata.dict(
                    by_alias=True,
                    exclude={"publication_date", "doi", "prereserve_doi"},
                )
            }
        )

        async with self.session.post(
            url, params=params, data=data, headers=headers
        ) as response:
            # Ignore content type
            return Deposition(**await response.json(content_type=None))

    def changed(self) -> bool:
        """Whether there are changes between the old deposition and the new one."""
        return self.uploads or self.deletes

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
        for name, resource in resources.items():
            filepath = resource.local_path
            if name not in self.deposition_files:
                logger.info(f"Adding {name} to deposition.")

                self.uploads.append(_UploadSpec(source=filepath, dest=filepath.name))
            else:
                file_info = self.deposition_files[name]

                # If file is not exact match for existing file, update with new file
                if not _compute_md5(resource.local_path) == file_info.checksum:
                    logger.info(f"Updating {name}")
                    self.deletes.append(file_info)
                    self.uploads.append(
                        _UploadSpec(source=filepath, dest=filepath.name)
                    )

        # Delete files not included in new deposition
        for filename, file_info in self.deposition_files.items():
            if filename not in resources and filename != "datapackage.json":
                self.deletes.append(file_info)

        await self.update_datapackage(resources)

        await self._apply_changes()

    async def _apply_changes(self):
        """Actually upload and delete what we listed in self.uploads/deletes."""
        logger.info(f"To delete: {self.deletes}")
        logger.info(f"To upload: {self.uploads}")
        if self.dry_run:
            logger.info("Dry run, aborting.")
            return
        for file_info in self.deletes:
            await self.delete_file(file_info)
        for upload in self.uploads:
            if isinstance(upload.source, io.IOBase):
                await self.upload(upload.source, upload.dest)
            else:
                with upload.source.open("rb") as f:
                    await self.upload(f, upload.dest)

    async def get_deposition(self, concept_doi: str):
        """Get data for a single Zenodo Deposition based on the provided query.

        See https://developers.zenodo.org for more information.

        Args:
            concept_doi: Concept DOI for desired data source.

        Returns:
            Deposition metadata pertaining to the latest version returned by Zenodo api.
        """
        url = f"{self.api_root}/deposit/depositions"
        params = {"q": f'conceptdoi:"{concept_doi}"', "access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            # Zenodo will return a list of depositions
            raw_json = await response.json()

            # By using the conceptdoi query there should only be a single deposition returned
            if len(raw_json) > 1:
                raise RuntimeError(
                    "Error Zenodo should only return a single deposition"
                )

            return Deposition(**raw_json[0])

    async def new_deposition_version(self):
        """Produce a new version for a given deposition archive.

        Returns:
            Deposition object, per
            https://developers.zenodo.org/?python#depositions
        """
        if not self.deposition.submitted:
            return self.deposition

        url = f"{self.api_root}/deposit/depositions/{self.deposition.id_}/actions/newversion"

        # Create the new version
        params = {"access_token": self.upload_key}
        async with self.session.post(url, params=params) as response:
            new_deposition = Deposition(**await response.json())

        # When the API creates a new version, it does not return the new one.
        # It returns the old one with a link to the new one.
        source_metadata = new_deposition.metadata.dict(by_alias=True)
        metadata = {}

        for key, val in source_metadata.items():
            if key not in ["doi", "prereserve_doi", "publication_date"]:
                metadata[key] = val

        previous = semantic_version.Version(source_metadata["version"])
        version_info = previous.next_major()

        metadata["version"] = str(version_info)

        # Update metadata of new deposition with new version info
        data = json.dumps({"metadata": metadata})

        # Get url to newest deposition
        deposition = await self.get_deposition(new_deposition.conceptdoi)
        url = deposition.links.self
        headers = {"Content-Type": "application/json"}

        async with self.session.put(
            url, params=params, data=data, headers=headers
        ) as response:
            return Deposition(**await response.json())

    async def delete_file(self, file: DepositionFile):
        """Delete file from zenodo deposition.

        Args:
            file: DepositionFile metadata pertaining to file to be deleted.
        """
        logger.info(f"Deleting file {file.filename} from zenodo deposition.")
        await self.session.delete(
            file.links.self, params={"access_token": self.upload_key}
        )

    async def upload(self, file: BinaryIO, filename: str):
        """Upload a file for the given deposition.

        Attempt using the bucket api and fall back on the file api.

        Args:
            file: File like object.
            filename: the desired file name.
        """
        params = {"access_token": self.upload_key}
        if self.new_deposition.links.bucket:
            url = f"{self.new_deposition.links.bucket}/{filename}"
        elif self.new_deposition.links.files:
            url = f"{self.new_deposition.links.files}/{filename}"
        else:
            raise RuntimeError("No file or bucket link available for deposition.")

        logger.info(f"Uploading file {filename} to zenodo deposition.")
        await self.session.put(url, params=params, data=file)

    async def update_datapackage(self, resources: dict[str, ResourceInfo]):
        """Create new frictionless datapackage for deposition.

        Args:
            resources: Dictionary mapping resources to ResourceInfo which is used to
            generate new datapackage
        Returns:
            Updated Deposition.
        """
        # If nothing has changed, don't add new datapackage
        if not self.changed():
            return None

        logger.info(f"Creating new datapackage.json for {self.data_source_id}")
        url = self.new_deposition.links.files
        params = {"access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            files = {
                file["filename"]: DepositionFile(**file)
                for file in await response.json()
            }

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

    async def publish(self):
        """Publish new deposition or discard if it hasn't been updated.

        Returns:
            Published Deposition.
        """
        if not self.changed():
            logger.info("Nothing changed, not publishing.")
            return

        if self.dry_run:
            logger.info(
                f"Dry run, not publishing uploads {self.uploads} and deletes {self.deletes}."
            )
            return

        logger.info(f"Publishing deposition for {self.data_source_id}")
        url = self.new_deposition.links.publish
        params = {"access_token": self.publish_key}
        headers = {"Content-Type": "application/json"}

        async with self.session.post(url, params=params, headers=headers) as response:
            return Deposition(**await response.json())


class ZenodoClient:
    """Thin interface to store data with zenodo.org via their API."""

    def __init__(
        self,
        deposition_settings: Path,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        testing: bool = False,
    ):
        """Initialize zenodo client interface."""
        # Load DOI's from settings file
        self.deposition_settings_path = deposition_settings
        with open(deposition_settings) as f:
            self.dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }

        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        self.testing = testing

        if testing:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

    @asynccontextmanager
    async def deposition_interface(
        self, data_source_id: str, initialize: bool = False, dry_run: bool = True
    ) -> ZenodoDepositionInterface:
        """Provides an async context manager that returns a ZenodoDepositionInterface.

        Args:
            data_source_id: Data source ID that will be used to generate zenodo metadata.
            initialize: Flag to create new deposition.
            dry_run: True skips all Zenodo writes.
        """
        if initialize:
            doi = None
        else:
            settings = self.dataset_settings[data_source_id]
            doi = settings.sandbox_doi if self.testing else settings.production_doi

        deposition_interface = await ZenodoDepositionInterface.open_interface(
            data_source_id,
            self.session,
            self.upload_key,
            self.publish_key,
            self.api_root,
            doi=doi,
            create_new=initialize,
            dry_run=dry_run,
        )

        try:
            yield deposition_interface
        except aiohttp.client_exceptions.ClientResponseError as e:
            # Log error and return to avoid interfering with other data sources
            logger.error(
                f"Received HTTP error while archiving {data_source_id} with status {e.status}: {e.message}"
            )
            return

        deposition = await deposition_interface.publish()

        if initialize:
            # Get new DOI and update settings
            if self.testing:
                sandbox_doi = deposition.conceptdoi
                production_doi = self.dataset_settings.get(
                    data_source_id, DatasetSettings()
                ).production_doi
            else:
                production_doi = deposition.conceptdoi
                sandbox_doi = self.dataset_settings.get(
                    data_source_id, DatasetSettings()
                ).sandbox_doi

            self.dataset_settings[data_source_id] = DatasetSettings(
                sandbox_doi=sandbox_doi, production_doi=production_doi
            )

            # Update doi settings YAML
            with open(self.deposition_settings_path, "w") as f:
                raw_settings = {
                    name: settings.dict()
                    for name, settings in self.dataset_settings.items()
                }
                yaml.dump(raw_settings, f)

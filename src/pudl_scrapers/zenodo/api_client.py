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

from pudl_scrapers.frictionless import DataPackage, ResourceInfo
from pudl_scrapers.zenodo.entities import (
    BucketFile,
    Deposition,
    DepositionFile,
    DepositionMetadata,
    Doi,
    SandboxDoi,
)


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
    """Thin interface to store data with zenodo.org via their API."""

    def __init__(
        self,
        name: str,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        api_root: str,
    ):
        """
        Prepare the ZenodoStorage interface.

        Args:

        Returns:
            ZenodoStorage

        """
        self.name = name

        self.api_root = api_root
        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        # Map resource name to resource partitions
        self.resource_parts: dict[str, dict] = {}

        self.deposition: Deposition = None
        self.new_deposition: Deposition = None
        self.deposition_files: dict[str, DepositionFile] = {}
        self.changed = False

    @classmethod
    async def open_interface(
        cls,
        name: str,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        api_root: str,
        doi: str | None = None,
        create_new: bool = False,
    ):
        """Create deposition interface to existing zenodo deposition."""
        interface = cls(name, session, upload_key, publish_key, api_root)

        if create_new:
            interface.deposition = await interface.create_deposition(name)
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
        """Return info on all files contained in deposition."""
        url = deposition.links.files
        params = {"access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            raw_json = await response.json()

        return {file["filename"]: DepositionFile(**file) for file in raw_json}

    async def create_deposition(self, data_source_id: str):
        """
        Create a Zenodo deposition resource.

        This should only be called once for a given data source.  The deposition will be
        prepared in draft form, so that files can be added prior to publication.

        Args:
            metadata: deposition metadata as a dict, per
            https://developers.zenodo.org/?python#representation

        Returns:
            deposition data as dict, per
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

    async def add_files(self, resources: dict[str, ResourceInfo]):
        """Check if local file already exists in deposition."""
        for name, resource in resources.items():
            if name not in self.deposition_files:
                self.changed = True
                self.logger.info(f"Uploading {name}")

                with open(resource.local_path, "rb") as f:
                    await self.upload(f, resource.local_path.name)
            else:
                file_info = self.deposition_files[name]

                # If file is not exact match for existing file, update with new file
                if not _compute_md5(resource.local_path) == file_info.checksum:
                    self.changed = True
                    self.logger.info(f"Updating {name}")
                    await self.delete_file(file_info)

                    with open(resource.local_path, "rb") as f:
                        await self.upload(f, resource.local_path.name)

        # Delete files not included in new deposition
        for filename, file in self.deposition_files.items():
            if filename not in resources and filename != "datapackage.json":
                self.changed = True
                await self.delete_file(file)

        await self.update_datapackage(resources)

    async def get_deposition(self, concept_doi: str):
        """
        Get data for a single Zenodo Deposition based on the provided query.

        See https://developers.zenodo.org for more information.

        Args:
            path_modifier: Modifier to base api path, such as 'deposition/depositions'
            query: A Zenodo (elasticsearch) compatible query string.
                         eg. 'title:"Eia860"' or 'doi:"10.5072/zenodo.415988"'

        Returns:

        """
        url = f"{self.api_root}/deposit/depositions"
        params = {"q": f'conceptdoi:"{concept_doi}"', "access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            # Deposition should be first element in json response
            raw_json = await response.json()

            return Deposition(**raw_json[0])

    async def new_deposition_version(self):
        """
        Produce a new version for a given deposition archive.

        Returns:
            deposition data as dict, per
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
        """Delete file from zenodo deposition."""
        await self.session.delete(
            file.links.self, params={"access_token": self.upload_key}
        )

    async def upload(self, file: BinaryIO, filename: str) -> BucketFile:
        """
        Upload a file for the given deposition.

        Attempt using the bucket api and fall back on the file api.

        Args:
            deposition: dict of the deposition resource
            file_name: the desired file name
            file_handle: an open file handle or bytes like object.
                Must be < 100MB

        Returns:
            dict of the deposition file resource, per
                https://developers.zenodo.org/#deposition-files
        """
        params = {"access_token": self.upload_key}
        if self.new_deposition.links.bucket:
            url = f"{self.new_deposition.links.bucket}/{filename}"
        elif self.new_deposition.links.files:
            url = f"{self.new_deposition.links.files}/{filename}"
        else:
            raise RuntimeError("No file or bucket link available for deposition.")

        async with self.session.put(url, params=params, data=file) as response:
            return await response.json()

    async def update_datapackage(self, resources: dict[str, ResourceInfo]):
        """Create new frictionless datapackage for deposition."""
        if not self.changed:
            return None

        self.logger.info(f"Creating new datapackage.json for {self.name}")
        url = self.new_deposition.links.files
        params = {"access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            files = {
                file["filename"]: DepositionFile(**file)
                for file in await response.json()
            }

        if "datapackage.json" in files:
            await self.delete_file(files["datapackage.json"])
            files.pop("datapackage.json")

        datapackage = DataPackage.from_filelist(self.name, files.values(), resources)

        datapackage_json = io.BytesIO(bytes(datapackage.json(), encoding="utf-8"))

        await self.upload(datapackage_json, "datapackage.json")

    async def publish(self):
        """Publish new deposition or discard if it hasn't been updated."""
        if not self.changed:
            return None

        self.logger.info(f"Publishing deposition for {self.name}")
        url = self.new_deposition.links.publish
        params = {"access_token": self.publish_key}
        headers = {"Content-Type": "application/json"}

        async with self.session.post(url, params=params, headers=headers) as response:
            await response.json()


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
        self, data_source_id: str, initialize: bool = False
    ) -> ZenodoDepositionInterface:
        """Return ZenodoDepositionInterface for data source in question."""
        logger = logging.getLogger(f"catalystcoop.{__name__}")

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
        )

        try:
            yield deposition_interface
        except aiohttp.client_exceptions.ClientResponseError as e:
            # Log error and return to avoid interfering with other data sources
            logger.error(f"Received HTTP error {e.status}: {e.message}")
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

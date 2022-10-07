"""Core routines for archiving raw data packages on Zenodo."""
import json
import logging
from hashlib import md5
from pathlib import Path

import aiohttp
import requests
import semantic_version
from pydantic import BaseModel

from pudl_scrapers.zenodo.entities import Deposition, DepositionFile, Doi, SandboxDoi


class DatasetSettings(BaseModel):
    """Simple model to validate doi's in settings."""

    porduction_doi: Doi | None = None
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

    async def __init__(
        self,
        session: aiohttp.ClientSession,
        upload_key: str,
        publish_key: str,
        concept_doi: str,
        testing=False,
    ):
        """
        Prepare the ZenodoStorage interface.

        Args:
            key (str): The API key required to authenticate with Zenodo
            testing (bool): If true, use the Zenodo sandbox api rather than the
                production service

        Returns:
            ZenodoStorage

        """
        self.logger = logging.Logger(f"catalystcoop.{__name__}")

        self.session = session

        self.upload_key = upload_key
        self.publish_key = publish_key

        if testing:
            # Validate DOI
            Doi.validate(concept_doi)
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            # Validate DOI
            SandboxDoi.validate(concept_doi)
            self.api_root = "https://zenodo.org/api"

        self.concept_doi = concept_doi
        self.deposition = await self.get_deposition(concept_doi)

        # Map file name to file metadata for all files in deposition
        self.deposition_files = {file.name: file for file in self.deposition.files}

        # Map resource name to resource partitions
        self.resource_parts: dict[str, dict] = {}

        # Create new deposition/flag indicating whether new deposition has been updated
        # If depostion is never updated, new deposition will be discarded
        self.new_depostion = await self.new_deposition_version()
        self.new_version = False

    async def add_file(self, file: Path, partitions: dict):
        """Check if local file already exists in deposition."""
        # Upload file if no version of it exists on latest deposition
        if str(file.name) not in self.deposition_files:
            self.new_version = True
            self.logger.info(f"Uploading {file.name}")
            await self.upload(file)
        else:
            file_info = self.deposition_files[str(file.name)]

            # If file is not exact match for existing file, update with new file
            if not _compute_md5(file) == file_info.checksum:
                self.new_version = True
                await self.delete_file(file_info)
                await self.upload(file)

        self.resource_parts[file.name] = partitions

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
        params = {"query": f"doi: {concept_doi}", "access_token": self.upload_key}

        async with self.session.get(url, params=params) as response:
            return await Deposition(**response.json())

    def create_deposition(self, metadata):
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
        url = self.api_root + "/deposit/depositions"
        params = {"access_token": self.key}
        headers = {"Content-Type": "application/json"}

        if metadata.get("version", None) is None:
            self.logger.debug(
                f"Deposition {metadata['title']} metadata assigned version 1.0.0"
            )
            metadata["version"] = "1.0.0"

        data = json.dumps({"metadata": metadata})

        response = requests.post(url, params=params, data=data, headers=headers)
        jsr = response.json()

        if response.status_code != 201:
            msg = f"Could not create deposition: {jsr}"
            self.logger.error(msg)
            raise RuntimeError(msg)

        return jsr

    async def new_deposition_version(self):
        """
        Produce a new version for a given deposition archive.

        Returns:
            deposition data as dict, per
            https://developers.zenodo.org/?python#depositions
        """
        if not self.deposition.submitted:
            raise RuntimeError(
                f"An unsubmitted deposition for {self.deposition.title} exists. Discard or publish before running."
            )

        url = f"{self.api_root}/deposit/depositions/{self.deposition.id_}/actions/newversion"

        # Create the new version
        params = {"access_token": self.key}
        async with self.session.post(url, params=params) as response:
            new_deposition = Deposition(**await response.json())

        # When the API creates a new version, it does not return the new one.
        # It returns the old one with a link to the new one.
        source_metadata = new_deposition.metadata.dict()
        metadata = {}

        for key, val in source_metadata.items():
            if key not in ["doi", "prereserve_doi", "publication_date"]:
                metadata[key] = val

        previous = semantic_version.Version(source_metadata["version"])
        version_info = previous.next_major()

        metadata["version"] = str(version_info)

        # Update metadata of new deposition with new version info
        data = json.dumps({"metadata": metadata})
        url = new_deposition.links.self

        async with self.session.put(url, params=params, data=data) as response:
            return Deposition(**await response.json())

    async def delete_file(self, file: DepositionFile):
        """Delete file from zenodo deposition."""
        await self.session.delete(
            file.links.self, params={"access_token": self.upload_key}
        )

    async def upload(self, file_path: Path) -> DepositionFile:
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
        url = f"{self.deposition.links.bucket}/{file_path.name}"

        with open(file_path, "rb") as f:
            async with self.session.put(url, params=params, data=f) as response:
                return DepositionFile(**await response.json())

    async def finish(self):
        """Publish new deposition or discard if it hasn't been updated."""
        if self.new_version:
            url = self.new_depostion.links.publish
            params = {"access_token": self.publish_key}
        else:
            url = self.new_depostion.links.discard
            params = {"access_token": self.upload_key}

        await self.session.post(url, params=params)

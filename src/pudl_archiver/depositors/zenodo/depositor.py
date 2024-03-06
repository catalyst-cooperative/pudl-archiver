"""Handle all deposition actions within Zenodo."""
import asyncio
import importlib
import json
import logging
import os
from hashlib import md5
from pathlib import Path
from typing import BinaryIO, Literal

import aiohttp
import semantic_version  # type: ignore  # noqa: PGH003
import yaml
from pydantic import BaseModel, PrivateAttr

from pudl_archiver.depositors.depositor import (
    AbstractDepositorInterface,
    DepositionAction,
    DepositionChange,
)
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import RunSettings, Url, retry_async

from .entities import (
    Deposition,
    DepositionMetadata,
    Doi,
    Record,
    SandboxDoi,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _compute_md5(file_path: Path) -> str:
    """Compute an md5 checksum to compare to files in zenodo deposition."""
    hash_md5 = md5()  # noqa: S324
    with Path.open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


class ZenodoClientError(Exception):
    """Captures the JSON error information from Zenodo."""

    def __init__(self, status, message, errors=None):
        """Constructor.

        Args:
            status: status message of response
            message: message of response
            errors: if any, list of errors returned by response
        """
        self.status = status
        self.message = message
        self.errors = errors

    def __str__(self):
        """Cast to string."""
        return repr(self)

    def __repr__(self):
        """But the kwargs are useful for recreating this object."""
        return f"ZenodoClientError(status={self.status}, message={self.message}, errors={self.errors})"


class DatasetSettings(BaseModel):
    """Simple model to validate doi's in settings."""

    production_doi: Doi | None = None
    sandbox_doi: SandboxDoi | None = None


class ZenodoDepositorInterface(AbstractDepositorInterface):
    """Implement AbstractDepositorInterface for zenodo depositions."""

    deposition: Deposition | None = None
    dataset_id: str
    settings: RunSettings

    # Private attributes
    _request = PrivateAttr()

    @classmethod
    async def get_latest_version(
        cls,
        dataset: str,
        session: aiohttp.ClientSession,
        run_settings: RunSettings,
    ) -> "AbstractDepositorInterface":
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self._request.
            run_settings: Settings from CLI.
        """
        self = cls(dataset_id=dataset, settings=run_settings)
        self._request = self._make_requester(session)

        if not self.settings.initialize:
            self = self.model_copy(
                update={"deposition": await self._get_deposition(self.doi)}
            )
        return self

    async def open_draft(self, create_new: bool) -> "AbstractDepositorInterface":
        """Open a new draft deposition to make edits."""
        if self.settings.initialize:
            deposition = await self._create_new_deposition()
        else:
            deposition = await self._get_new_version(
                clobber=True, refresh_metadata=self.settings.refresh_metadata
            )

        return self.model_copy(update={"deposition": deposition})

    async def publish(self) -> "AbstractDepositorInterface":
        """Publish draft deposition and return new depositor with updated deposition."""
        url = self.deposition.links.publish
        headers = {
            "Content-Type": "application/json",
        } | self.auth_actions
        response = await self._request(
            "POST", url, log_label="Publishing deposition", headers=headers
        )
        published = Deposition(**response)
        if self.create_new:
            self._update_dataset_settings(published)

        return self.model_copy(update={"deposition": published})

    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        # Return None if deposition is not initialized
        if not self.deposition:
            return None
        file_bytes = None
        if file_info := self.deposition.files_map.get(filename):
            url = file_info.links.canonical
            response = await self._request(
                "GET",
                url,
                f"Download {filename}",
                parse_json=False,
                headers=self.auth_write,
            )
            file_bytes = await retry_async(response.read)
        return file_bytes

    async def list_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        return list(self.deposition.files_map.keys())

    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        return self.deposition.links.html

    async def create_file(
        self,
        filename: str,
        data: BinaryIO,
        force_api: Literal["bucket", "files"] | None = None,
    ):
        """Create a file in a deposition.

        Args:
            filename: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        if self.deposition.links.bucket and force_api != "files":
            url = f"{self.deposition.links.bucket}/{filename}"
            await self._request(
                "PUT",
                url,
                log_label=f"Uploading {filename} to bucket",
                data=data,
                headers=self.auth_write,
                timeout=3600,
            )
        elif self.deposition.links.files and force_api != "bucket":
            url = f"{self.deposition.links.files}"
            await self._request(
                "POST",
                url,
                log_label=f"Uploading {filename} to files API",
                data={"file": data, "name": filename},
                headers=self.auth_write,
            )
        else:
            raise RuntimeError("No file or bucket link available for deposition.")
        self.deposition = await self._get_deposition_by_id(self.deposition.id_)

    async def delete_file(
        self,
        filename: str,
    ):
        """Delete a file from a deposition.

        Args:
            filename: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        if not (file_to_delete := self.deposition.files_map.get(filename)):
            logger.info(f"No files matched {filename}.")
            return

        await self._request(
            "DELETE",
            file_to_delete.links.self,
            parse_json=False,
            log_label=f"Deleting {filename} from deposition {self.deposition.id_}",
            headers=self.auth_write,
        )
        self.deposition = await self._get_deposition_by_id(self.deposition.id_)

    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange | None:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        action = None
        if file_info := self.deposition.files_map.get(filename):
            # If file is not exact match for existing file, update with new file
            if (local_md5 := _compute_md5(resource.local_path)) != file_info.checksum:
                logger.info(
                    f"Updating {filename}: local hash {local_md5} vs. remote {file_info.checksum}"
                )
                action = DepositionAction.UPDATE
            else:
                logger.info(f"Adding {filename} to deposition.")

                action = DepositionAction.CREATE
        else:
            logger.info(f"Adding {filename} to deposition.")

            action = DepositionAction.CREATE

        if action is None:
            return None

        return DepositionChange(
            action_type=action,
            name=filename,
            resource=resource.local_path,
        )

    def generate_datapackage(self, resources: dict[str, ResourceInfo]) -> DataPackage:
        """Generate new datapackage, attach to deposition, and return."""
        logger.info(f"Creating new datapackage.json for {self.data_source_id}")

        # Create updated datapackage
        datapackage = DataPackage.from_filelist(
            self.data_source_id,
            [f for f in self.deposition.files if f.filename != "datapackage.json"],
            resources,
            self.deposition.metadata.version,
        )

        return datapackage

    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        logger.error(f"Failed while creating new deposition: {e}")

    async def _create_new_deposition(self) -> Deposition:
        metadata = DepositionMetadata.from_data_source(self.dataset_id)
        if not metadata.keywords:
            raise AssertionError(
                "New dataset is missing keywords and cannot be archived."
            )

        url = f"{self.api_root}/deposit/depositions"
        headers = {
            "Content-Type": "application/json",
        } | self.auth_write

        payload = {
            "metadata": metadata.dict(
                by_alias=True,
                exclude={"publication_date", "doi", "prereserve_doi"},
            )
        }

        response = await self._request(
            "POST", url, "Create new deposition", json=payload, headers=headers
        )
        # Ignore content type
        return Deposition(**response)

    async def _get_new_version(
        self,
        clobber: bool = False,
        refresh_metadata: bool = False,
    ) -> Deposition:
        """Get a new version of a deposition.

        1. Get a fresh new draft based on the deposition.
        2. Increment its major version number.

        Guaranteeing a fresh new draft that looks like the existing record is
        surprisingly difficult, due to instability in Zenodo's deposition
        querying functionality.

        1. Try to create a new version of the deposition.
        2. If that fails, that is because there is already a draft.
        3. Since the existing draft may have been changed from its initial state, delete it.
           a. The only way to find the existing draft is to use the undocumented `POST /records/:id:/versions` API endpoint, which will either create a new draft or return an existing one.
           b. We need to extract the deposition ID from the above output in order to delete it.
        4.

        Args:
            clobber: if there is an existing draft, delete it and get a new one.
            refresh_metadata: regenerate metadata from data source rather than existing
                archives' metadata.

        Returns:
            A new Deposition that is a snapshot of the old one you passed in,
            with a new major version number.
        """
        # just get the new draft from new API.
        new_draft_record = Record(
            **await self._request(
                "POST",
                f"{self.api_root}/records/{self.deposition.id_}/versions",
                log_label=f"Get existing draft deposition for {self.deposition.id_}",
                headers=self.auth_write,
            )
        )
        # then, get the deposition based on that ID.
        new_draft_deposition = await self._get_deposition_by_id(new_draft_record.id_)

        # Update metadata of new deposition with new version info
        base_metadata = self.deposition.metadata.model_dump(by_alias=True)

        if refresh_metadata:
            logger.info("Re-creating deposition metadata from PUDL source data.")
            draft_metadata = DepositionMetadata.from_data_source(self.dataset_id).dict(
                by_alias=True
            )
        else:
            draft_metadata = new_draft_deposition.metadata.model_dump(by_alias=True)

        metadata = {
            key: val
            for key, val in (base_metadata | draft_metadata).items()
            if key not in {"doi", "prereserve_doi", "publication_date"}
        }
        base_version = semantic_version.Version(base_metadata["version"])
        new_version = base_version.next_major()
        metadata["version"] = str(new_version)
        logging.info(f"{base_metadata=}\n{draft_metadata=}\n{metadata=}")
        data = json.dumps({"metadata": metadata})
        # Get url to newest deposition
        new_deposition_url = new_draft_deposition.links.latest_draft
        headers = {
            "Content-Type": "application/json",
        } | self.auth_write
        response = await self._request(
            "PUT",
            new_deposition_url,
            log_label=f"Updating version number from {base_version} "
            f"(from {self.deposition.id_}) to {new_version} "
            f"(on {new_draft_deposition.id_})",
            data=data,
            headers=headers,
        )
        return Deposition(**response)

    @property
    def doi(self):
        """Return DOI from settings."""
        dataset_settings_path = (
            importlib.resources.files("pudl_archiver.package_data") / "zenodo_doi.yaml"
        )
        with Path.open(dataset_settings_path) as f:
            dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }
        doi_settings = dataset_settings[self.dataset_id]
        if self.settings.sandbox:
            doi = doi_settings.sandbox_doi
        else:
            doi = doi_settings.production_doi
        return doi

    @property
    def auth_write(self):
        """Format auth header with upload_key."""
        if self.settings.sandbox:
            upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        else:
            upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
        return {"Authorization": f"Bearer {upload_key}"}

    @property
    def auth_actions(self):
        """Format auth header with publish_key."""
        if self.settings.sandbox:
            publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
        else:
            publish_key = os.environ["ZENODO_TOKEN_PUBLISH"]
        return {"Authorization": f"Bearer {publish_key}"}

    @property
    def api_root(self):
        """Return base URL for zenodo server (sandbox or production)."""
        if self.settings.sandbox:
            api_root = "https://sandbox.zenodo.org/api"
        else:
            api_root = "https://zenodo.org/api"
        return api_root

    async def _get_deposition(self, concept_doi: str) -> Deposition:
        """Get the latest deposition associated with a concept DOI.

        Sometimes the deposition information that comes back from the concept
        DOI query is incomplete, so we use the record ID that is returned from
        that to make another request for the "full" data.

        Args:
            concept_doi: the DOI for the concept - gets the latest associated
                DOI.
            published_only: if True, only returns depositions that have been
                published.

        Returns:
            The latest deposition associated with the concept DOI.
        """
        concept_rec_id = concept_doi.split(".")[2]
        record = Record(
            **await self._request(
                "GET",
                f"{self.api_root}/records/{concept_rec_id}",
                headers=self.auth_write,
                log_label=f"Get latest record for {concept_doi}",
            )
        )
        return await self._get_deposition_by_id(record.id_)

    async def _get_deposition_by_id(self, rec_id: int) -> Deposition:
        """Get a deposition by its record ID directly instead of through concept.

        Args:
            rec_id: The record ID of the deposition you would like to get.
        """
        response = await self._request(
            "GET",
            f"{self.api_root}/deposit/depositions/{rec_id}",
            log_label=f"Get deposition for {rec_id}",
            headers=self.auth_write,
        )
        deposition = Deposition(**response)
        logger.debug(deposition)
        return deposition

    def _make_requester(self, session):
        """Wraps our session requests with some Zenodo-specific error handling."""

        async def requester(
            method: Literal["GET", "POST", "PUT", "DELETE"],
            url: str,
            log_label: str,
            parse_json: bool = True,
            retry_count: int = 7,
            **kwargs,
        ) -> dict | aiohttp.ClientResponse:
            """Make requests to Zenodo.

            Args:
                method: HTTP method - "GET", "POST", etc.
                url: the URL you are going to hit.
                log_label: a string describing what this request does, for
                    logging purposes.
                parse_json: whether or not to always parse the response as a
                    JSON object. Default to True.

            Returns:
                Either the parsed JSON or the raw aiohttp.ClientResponse object.
            """
            logger.info(f"{method} {url} - {log_label}")

            async def run_request():
                # Convert all urls to str to in case they are pydantic Url types
                response = await session._request(method, str(url), **kwargs)
                if response.status >= 400:
                    if response.headers["Content-Type"] == "application/json":
                        json_resp = await response.json()
                        raise ZenodoClientError(
                            status=response.status,
                            message=json_resp.get("message"),
                            errors=json_resp.get("errors"),
                        )
                    message = await response.text()
                    raise ZenodoClientError(
                        status=response.status,
                        message=message,
                    )
                if parse_json:
                    return await response.json()
                return response

            response = await retry_async(
                run_request,
                retry_on=(
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    ZenodoClientError,
                ),
                retry_count=retry_count,
            )
            return response

        return requester

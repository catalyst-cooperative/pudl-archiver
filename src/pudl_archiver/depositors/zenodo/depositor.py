"""Handle all deposition actions within Zenodo."""

import asyncio
import importlib
import json
import logging
import os
import traceback
from pathlib import Path
from typing import BinaryIO, Literal

import aiohttp
import semantic_version  # type: ignore  # noqa: PGH003
import yaml
from pydantic import BaseModel, PrivateAttr

from pudl_archiver.depositors.depositor import (
    DepositionAction,
    DepositionChange,
    DepositorAPIClient,
    DraftDeposition,
    PublishedDeposition,
    register_depositor,
)
from pudl_archiver.frictionless import (
    MEDIA_TYPES,
    DataPackage,
    Partitions,
    Resource,
    ResourceInfo,
)
from pudl_archiver.utils import RunSettings, Url, compute_md5, retry_async

from .entities import (
    Deposition,
    DepositionFile,
    DepositionMetadata,
    Doi,
    Record,
    SandboxDoi,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")


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


def _resource_from_file(file: DepositionFile, parts: dict[str, str]) -> Resource:
    """Create a resource from a single file with partitions.

    Args:
        file: Deposition file metadata returned by Zenodo api.
        parts: Working partitions of current resource.
    """
    filename = Path(file.filename)
    mt = MEDIA_TYPES[filename.suffix[1:]]

    return Resource(
        name=file.filename,
        path=file.links.canonical,
        remote_url=file.links.canonical,
        title=filename.name,
        mediatype=mt,
        parts=parts,
        bytes=file.filesize,
        hash=file.checksum,
        format=filename.suffix,
    )


class ZenodoAPIClient(DepositorAPIClient):
    """Implements the base interface to zenodo depositions.

    This class will be inherited by both the Draft and Published Zenodo deposition
    classes to avoid duplicated code between the two. Only the Draft version is
    writeable, so this interface will be read only.
    """

    sandbox: bool

    # Private attributes
    _request = PrivateAttr()
    _dataset_settings_path = PrivateAttr()
    _session = PrivateAttr()

    @classmethod
    async def initialize_client(
        cls,
        session: aiohttp.ClientSession,
        sandbox: bool,
        deposition_path: str | None = None,
    ) -> "ZenodoAPIClient":
        """Initialize API client connection.

        Args:
            session: HTTP handler - we don't use it directly, it's wrapped in self._request.
        """
        if deposition_path:
            raise RuntimeError(
                "Zenodo depositor does not use deposition_path parameter."
            )

        self = cls(sandbox=sandbox)
        self._session = session
        self._request = self._make_requester(session)
        self._dataset_settings_path = (
            importlib.resources.files("pudl_archiver.package_data") / "zenodo_doi.yaml"
        )
        return self

    async def get_file(self, deposition: Deposition, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        # Return None if deposition is not initialized
        if not deposition:
            return None
        file_bytes = None
        if file_info := deposition.files_map.get(filename):
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

    async def list_files(self, deposition: Deposition) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        return list(deposition.files_map.keys())

    def get_deposition_link(self, deposition: Deposition) -> Url:
        """Get URL which points to deposition."""
        return deposition.links.html

    async def create_file(
        self,
        deposition: Deposition,
        filename: str,
        data: BinaryIO,
        force_api: Literal["bucket", "files"] | None = None,
    ) -> Deposition:
        """Create a file in a deposition.

        Args:
            filename: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            Remote deposition after creating file.
        """
        if deposition.links.bucket and force_api != "files":
            url = f"{deposition.links.bucket}/{filename}"
            await self._request(
                "PUT",
                url,
                log_label=f"Uploading {filename} to bucket",
                data=data,
                headers=self.auth_write,
                timeout=3600,
            )
        elif deposition.links.files and force_api != "bucket":
            url = f"{deposition.links.files}"
            await self._request(
                "POST",
                url,
                log_label=f"Uploading {filename} to files API",
                data={"file": data, "name": filename},
                headers=self.auth_write,
            )
        else:
            raise RuntimeError("No file or bucket link available for deposition.")

        return await self.get_deposition_by_id(deposition.id_)

    async def delete_file(
        self,
        deposition: Deposition,
        filename: str,
    ) -> "ZenodoDraftDeposition":
        """Delete a file from a deposition.

        Args:
            filename: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        if not (file_to_delete := deposition.files_map.get(filename)):
            logger.info(f"No files matched {filename}; could not delete.")
            return None

        await self._request(
            "DELETE",
            file_to_delete.links.self,
            parse_json=False,
            log_label=f"Deleting {filename} from deposition {deposition.id_}",
            headers=self.auth_write,
        )

        return await self.get_deposition_by_id(deposition.id_)

    async def delete_deposition(self, deposition: Deposition) -> None:
        """Delete an un-submitted deposition.

        As of 2023-11-22, Zenodo API times out on first few deletion attempts,
        occasionally 500s, and then 404s once the delete has actually gone
        through.

        Args:
            deposition: the deposition you want to delete.
        """
        try:
            await self._request(
                "DELETE",
                deposition.links.self,
                log_label="Deleting deposition",
                headers=self.auth_write,
                parse_json=False,
                retry_count=5,
            )
        except ZenodoClientError as e:
            if e.status != 404:
                raise e
            logger.info(
                f"404 Not Found when deleting {deposition.links.self}, assume "
                "earlier delete succeeded."
            )

    async def publish(self, deposition: Deposition) -> Deposition:
        """Publish draft deposition and return new depositor with updated deposition."""
        url = deposition.links.publish
        headers = {
            "Content-Type": "application/json",
        } | self.auth_actions
        response = await self._request(
            "POST", url, log_label="Publishing deposition", headers=headers
        )
        return Deposition(**response)

    async def create_new_deposition(self, dataset_id: str) -> Deposition:
        """Create a brand new empty draft deposition from dataset_id."""
        metadata = DepositionMetadata.from_data_source(dataset_id)
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

    def update_dataset_settings(self, dataset_id: str, published_deposition):
        """Update settings with new DOI."""
        # Get new DOI and update settings
        # TODO (daz): split this IO out too.
        dataset_settings = self.dataset_settings
        if self.sandbox:
            sandbox_doi = published_deposition.conceptdoi
            production_doi = dataset_settings.get(
                dataset_id, DatasetSettings()
            ).production_doi
        else:
            production_doi = published_deposition.conceptdoi
            sandbox_doi = dataset_settings.get(
                dataset_id, DatasetSettings()
            ).sandbox_doi

        dataset_settings[dataset_id] = DatasetSettings(
            sandbox_doi=sandbox_doi, production_doi=production_doi
        )

        # Update doi settings YAML
        with Path.open(self._dataset_settings_path, "w") as f:
            raw_settings = {
                name: settings.dict() for name, settings in dataset_settings.items()
            }
            yaml.dump(raw_settings, f)

    async def get_new_version(
        self,
        dataset_id: str,
        published_deposition: Deposition,
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
            published_deposition: Deposition object from previous published version.
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
                f"{self.api_root}/records/{published_deposition.id_}/versions",
                log_label=f"Get existing draft deposition for {published_deposition.id_}",
                headers=self.auth_write,
            )
        )
        # then, get the deposition based on that ID.
        new_draft_deposition = await self.get_deposition_by_id(new_draft_record.id_)

        # Update metadata of new deposition with new version info
        base_metadata = published_deposition.metadata.model_dump(by_alias=True)

        if refresh_metadata:
            logger.info("Re-creating deposition metadata from PUDL source data.")
            draft_metadata = DepositionMetadata.from_data_source(dataset_id).dict(
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
            f"(from {published_deposition.id_}) to {new_version} "
            f"(on {new_draft_deposition.id_})",
            data=data,
            headers=headers,
        )
        return Deposition(**response)

    @property
    def dataset_settings(self):
        """Load doi settings from yaml file and return."""
        with Path.open(self._dataset_settings_path) as f:
            dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }
        return dataset_settings

    def doi(self, dataset_id: str):
        """Return DOI from settings."""
        dataset_settings = self.dataset_settings.get(dataset_id, DatasetSettings())
        if self.sandbox:
            doi = dataset_settings.sandbox_doi
        else:
            doi = dataset_settings.production_doi
        return doi

    @property
    def auth_write(self):
        """Format auth header with upload_key."""
        if self.sandbox:
            upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
        else:
            upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
        return {"Authorization": f"Bearer {upload_key}"}

    @property
    def auth_actions(self):
        """Format auth header with publish_key."""
        if self.sandbox:
            publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
        else:
            publish_key = os.environ["ZENODO_TOKEN_PUBLISH"]
        return {"Authorization": f"Bearer {publish_key}"}

    @property
    def api_root(self):
        """Return base URL for zenodo server (sandbox or production)."""
        if self.sandbox:
            api_root = "https://sandbox.zenodo.org/api"
        else:
            api_root = "https://zenodo.org/api"
        return api_root

    async def get_deposition(self, dataset_id: str) -> Deposition:
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
        concept_doi = self.doi(dataset_id)
        concept_rec_id = concept_doi.split(".")[2]
        try:
            record = Record(
                **await self._request(
                    "GET",
                    f"{self.api_root}/records/{concept_rec_id}",
                    headers=self.auth_write,
                    log_label=f"Get latest record for {concept_doi}",
                )
            )
        except ZenodoClientError as e:
            # Recurring bug as of Feb 2025.
            # Sometimes, Zenodo marks the concept DOI as deleted, preventing us from
            # updating archives. To get around this, we can use the fact that the very
            # first record of an archive is the concept DOI + 1. Zenodo provides a /latest
            # endpoint that redirects to the latest version in a repository, so we can
            # hack our way to the latest version through this alternative path.
            if e.message == "The record has been deleted." and e.status == 410:
                logger.warn(
                    f"Got Zenodo concept record deletion error: {e.message}. Attempting alternate method to get latest record."
                )
                first_version_id = str(
                    int(concept_rec_id) + 1
                )  # First record is one larger than the concept DOI
                record = Record(
                    **await self._request(
                        "GET",
                        f"{self.api_root}/records/{first_version_id}/versions/latest",
                        headers=self.auth_write,
                        log_label=f"Get latest record for {first_version_id}",
                    )
                )
            else:
                raise e

        return await self.get_deposition_by_id(record.id_)

    async def get_deposition_by_id(self, rec_id: int) -> Deposition:
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


class ZenodoPublishedDeposition(PublishedDeposition):
    """Interface to Published Zenodo deposition."""

    deposition: Deposition
    settings: RunSettings
    api_client: ZenodoAPIClient
    dataset_id: str

    async def open_draft(self) -> "ZenodoDraftDeposition":
        """Open a new draft deposition to make edits."""
        draft_deposition = await self.api_client.get_new_version(
            self.dataset_id,
            self.deposition,
            clobber=True,
            refresh_metadata=self.settings.refresh_metadata,
        )
        return ZenodoDraftDeposition(
            deposition=draft_deposition,
            settings=self.settings,
            dataset_id=self.dataset_id,
            api_client=self.api_client,
        )

    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        return self.api_client.get_deposition_link(self.deposition)

    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        return await self.api_client.get_file(self.deposition, filename)

    async def list_files(self) -> list[str]:
        """Return list of filenames from published version of deposition."""
        return await self.api_client.list_files(self.deposition)


class ZenodoDraftDeposition(DraftDeposition):
    """Implement AbstractDepositorInterface for zenodo depositions."""

    deposition: Deposition
    settings: RunSettings
    dataset_id: str
    api_client: ZenodoAPIClient

    async def publish(self) -> ZenodoPublishedDeposition:
        """Publish draft deposition and return new depositor with updated deposition."""
        published = await self.api_client.publish(self.deposition)
        if self.settings.initialize:
            self.api_client.update_dataset_settings(self.dataset_id, published)

        # Get Published deposition
        return ZenodoPublishedDeposition(
            deposition=published,
            dataset_id=self.dataset_id,
            api_client=self.api_client,
            settings=self.settings.model_copy(update={"initialize": False}),
        )

    async def create_file(
        self,
        filename: str,
        data: BinaryIO,
        force_api: Literal["bucket", "files"] | None = None,
    ) -> "ZenodoDraftDeposition":
        """Create a file in a deposition.

        Args:
            filename: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        return self.model_copy(
            update={
                "deposition": await self.api_client.create_file(
                    self.deposition,
                    filename,
                    data,
                )
            }
        )

    async def delete_file(
        self,
        filename: str,
    ) -> "ZenodoDraftDeposition":
        """Delete a file from a deposition.

        Args:
            filename: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        return self.model_copy(
            update={
                "deposition": await self.api_client.delete_file(
                    self.deposition, filename
                )
            }
        )

    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        return self.api_client.get_deposition_link(self.deposition)

    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        return await self.api_client.get_file(self.deposition, filename)

    def get_checksum(self, filename: str) -> str | None:
        """Get checksum for a file in the current deposition.

        Args:
            filename: Name of file to checksum.
        """
        file_info = self.deposition.files_map.get(filename)
        return file_info.checksum if file_info else None

    async def list_files(self) -> list[str]:
        """Return list of filenames from published version of deposition."""
        return await self.api_client.list_files(self.deposition)

    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        action = DepositionAction.NO_OP
        if file_info := self.deposition.files_map.get(filename):
            # If file is not exact match for existing file, update with new file
            if (local_md5 := compute_md5(resource.local_path)) != file_info.checksum:
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

        return DepositionChange(
            action_type=action,
            name=filename,
            resource=resource.local_path,
        )

    def generate_datapackage(
        self, partitions_in_deposition: dict[str, Partitions]
    ) -> DataPackage:
        """Generate new datapackage, attach to deposition, and return."""
        logger.info(f"Creating new datapackage.json for {self.dataset_id}")

        # Create updated datapackage
        resources = [
            _resource_from_file(f, partitions_in_deposition[f.filename])
            for f in self.deposition.files
            if f.filename != "datapackage.json"
        ]
        datapackage = DataPackage.new_datapackage(
            self.dataset_id,
            resources,
            self.deposition.metadata.version,
        )

        return datapackage

    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        logger.error(
            f"Failed while creating new deposition: {traceback.print_exception(e)}"
        )

    async def delete_deposition(self) -> None:
        """Delete an un-submitted deposition."""
        return await self.api_client.delete_deposition(self.deposition)


register_depositor(
    "zenodo", ZenodoAPIClient, ZenodoPublishedDeposition, ZenodoDraftDeposition
)

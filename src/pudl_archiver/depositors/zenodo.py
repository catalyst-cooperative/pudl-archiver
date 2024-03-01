"""Handle all deposition actions within Zenodo."""
import asyncio
import json
import logging
import os
from hashlib import md5
from pathlib import Path
from typing import BinaryIO, Literal

import aiohttp
import semantic_version  # type: ignore  # noqa: PGH003
import yaml
from pydantic import BaseModel

from pudl_archiver import checkpoints
from pudl_archiver.depositors.depositor import (
    AbstractDepositor,
    DepositionAction,
    DepositionChange,
)
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import Url, retry_async
from pudl_archiver.zenodo.entities import (
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


class ZenodoDepositor(AbstractDepositor):
    """Act on depositions & deposition files within Zenodo."""

    def __init__(
        self,
        dataset: str,
        session: aiohttp.ClientSession,
        dataset_settings_path: Path,
        sandbox: bool = True,
    ):
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            sandbox: whether to hit the sandbox Zenodo instance or the real one. Default True.
        """
        self.dataset_settings_path = dataset_settings_path
        if sandbox:
            upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
            publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            upload_key = os.environ["ZENODO_TOKEN_UPLOAD"]
            publish_key = os.environ["ZENODO_TOKEN_PUBLISH"]
            self.api_root = "https://zenodo.org/api"

        self.auth_write = {"Authorization": f"Bearer {upload_key}"}
        self.auth_actions = {"Authorization": f"Bearer {publish_key}"}
        self.request = self._make_requester(session)

        self.sandbox = sandbox
        self.data_source_id = dataset

        with Path.open(self.dataset_settings_path) as f:
            self.dataset_settings = {
                name: DatasetSettings(**dois)
                for name, dois in yaml.safe_load(f).items()
            }

    async def prepare_depositor(
        self, create_new: bool, resume_run: bool, refresh_metadata: bool
    ) -> dict[str, ResourceInfo]:
        """Perform any async setup necessary for depositor.

        Args:
            create_new: whether or not we are initializing a new Zenodo Concept DOI.
            resume_run: Attempt to resume a run that was previously interrupted.
            refresh_metadata: Regenerate metadata from PUDL data source rather than
                existing archived metadata.
        """
        self.create_new = create_new
        if resume_run:
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
                draft = await self.get_new_version(
                    original,
                    clobber=True,
                    data_source_id=self.data_source_id,
                    refresh_metadata=refresh_metadata,
                )

        logger.info(f"ORIGINAL FILES: {original.files_map}")
        logger.info(f"DRAFT FILES: {original.files_map}")
        self.deposition = draft
        return existing_resources

    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange | None:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        action = None
        logger.info(f"EXISTING FILES: {self.deposition.files_map}")
        if filename not in self.deposition.files_map:
            logger.info(f"Adding {filename} to deposition.")

            action = DepositionAction.CREATE
        else:
            file_info = self.deposition.files_map[filename]

            # If file is not exact match for existing file, update with new file
            if (local_md5 := _compute_md5(resource.local_path)) != file_info.checksum:
                logger.info(
                    f"Updating {filename}: local hash {local_md5} vs. remote {file_info.checksum}"
                )
                action = DepositionAction.UPDATE

        if action is None:
            return None

        return DepositionChange(
            action_type=action,
            name=filename,
            resource=resource.local_path,
        )

    async def _create_new_deposition(self) -> Deposition:
        metadata = DepositionMetadata.from_data_source(self.data_source_id)
        if not metadata.keywords:
            raise AssertionError(
                "New dataset is missing keywords and cannot be archived."
            )
        return await self.create_deposition(metadata)

    async def _get_existing_deposition(
        self, dataset_settings: dict[str, DatasetSettings], data_source_id: str
    ) -> Deposition:
        settings = dataset_settings[data_source_id]
        doi = settings.sandbox_doi if self.sandbox else settings.production_doi
        if not doi:
            raise RuntimeError("Must pass a valid DOI if create_new is False")
        return await self.get_deposition(doi)

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
                response = await session.request(method, str(url), **kwargs)
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

    async def create_deposition(self, metadata: DepositionMetadata) -> Deposition:
        """Create a whole new deposition.

        Args:
            metadata: a metadata, to make a deposition with.

        Returns:
            The brand new deposition - unpublished and with no files in it.
        """
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

        response = await self.request(
            "POST", url, "Create new deposition", json=payload, headers=headers
        )
        # Ignore content type
        return Deposition(**response)

    async def get_deposition(
        self, concept_doi: str, published_only: bool = False
    ) -> Deposition:
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
            **await self.request(
                "GET",
                f"{self.api_root}/records/{concept_rec_id}",
                headers=self.auth_write,
                log_label=f"Get latest record for {concept_doi}",
            )
        )
        return await self.get_deposition_by_id(record.id_)

    async def get_deposition_by_id(self, rec_id: int) -> Deposition:
        """Get a deposition by its record ID directly instead of through concept.

        Args:
            rec_id: The record ID of the deposition you would like to get.
        """
        response = await self.request(
            "GET",
            f"{self.api_root}/deposit/depositions/{rec_id}",
            log_label=f"Get deposition for {rec_id}",
            headers=self.auth_write,
        )
        deposition = Deposition(**response)
        logger.debug(deposition)
        return deposition

    async def get_new_version(
        self,
        deposition: Deposition,
        data_source_id: str,
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
            deposition: the deposition you want to get the new version of.
            data_source_id: the deposition dataset name (to clobber existing metadata).
            clobber: if there is an existing draft, delete it and get a new one.
            refresh_metadata: regenerate metadata from data source rather than existing
                archives' metadata.

        Returns:
            A new Deposition that is a snapshot of the old one you passed in,
            with a new major version number.
        """
        # just get the new draft from new API.
        new_draft_record = Record(
            **await self.request(
                "POST",
                f"{self.api_root}/records/{deposition.id_}/versions",
                log_label=f"Get existing draft deposition for {deposition.id_}",
                headers=self.auth_write,
            )
        )
        # then, get the deposition based on that ID.
        new_draft_deposition = await self.get_deposition_by_id(new_draft_record.id_)

        # Update metadata of new deposition with new version info
        base_metadata = deposition.metadata.model_dump(by_alias=True)

        if refresh_metadata:
            logger.info("Re-creating deposition metadata from PUDL source data.")
            draft_metadata = DepositionMetadata.from_data_source(data_source_id).dict(
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
        response = await self.request(
            "PUT",
            new_deposition_url,
            log_label=f"Updating version number from {base_version} "
            f"(from {deposition.id_}) to {new_version} "
            f"(on {new_draft_deposition.id_})",
            data=data,
            headers=headers,
        )
        return Deposition(**response)

    async def publish_deposition(self) -> Deposition:
        """Publish a deposition.

        Needs to have at least one file, and needs to not already be published.

        Args:
            deposition: the deposition you want to publish.
        """
        url = self.deposition.links.publish
        headers = {
            "Content-Type": "application/json",
        } | self.auth_actions
        response = await self.request(
            "POST", url, log_label="Publishing deposition", headers=headers
        )
        published = Deposition(**response)
        if self.create_new:
            self._update_dataset_settings(published)

    async def delete_deposition(self, deposition) -> None:
        """Delete an un-submitted deposition.

        As of 2023-11-22, Zenodo API times out on first few deletion attempts,
        occasionally 500s, and then 404s once the delete has actually gone
        through.

        Args:
            deposition: the deposition you want to delete.
        """
        try:
            await self.request(
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

    async def delete_file(self, target: str) -> None:
        """Delete a file from a deposition.

        Args:
            target: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        if not (file_to_delete := self.deposition.files_map.get(target)):
            logger.info(f"No files matched {target}.")
            return None

        response = await self.request(
            "DELETE",
            file_to_delete.links.self,
            parse_json=False,
            log_label=f"Deleting {target} from deposition {self.deposition.id_}",
            headers=self.auth_write,
        )
        return response

    async def create_file(
        self,
        target: str,
        data: BinaryIO,
        force_api: Literal["bucket", "files"] | None = None,
    ) -> None:
        """Create a file in a deposition.

        Attempts to use the new "bucket" API over the "files" API, but you can
        force it to use "files" if desired.

        Args:
            target: the filename of the file you want to create.
            data: the actual data associated with the file.
            force_api: force using one files API over another. The options are
                "bucket" and "files"

        Returns:
            None if success.
        """
        if self.deposition.links.bucket and force_api != "files":
            url = f"{self.deposition.links.bucket}/{target}"
            return await self.request(
                "PUT",
                url,
                log_label=f"Uploading {target} to bucket",
                data=data,
                headers=self.auth_write,
                timeout=3600,
            )
        if self.deposition.links.files and force_api != "bucket":
            url = f"{self.deposition.links.files}"
            return await self.request(
                "POST",
                url,
                log_label=f"Uploading {target} to files API",
                data={"file": data, "name": target},
                headers=self.auth_write,
            )
        raise RuntimeError("No file or bucket link available for deposition.")

    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        file_bytes = None
        if file_info := self.deposition.files_map.get(filename):
            url = file_info.links.canonical
            response = await self.request(
                "GET",
                url,
                f"Download {filename}",
                parse_json=False,
                headers=self.auth_write,
            )
            file_bytes = await retry_async(response.read)
        return file_bytes

    async def update_datapackage(
        self,
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
        # Get refreshed deposition after all modifications
        self.deposition = await self.get_deposition_by_id(self.deposition.id_)

        old_datapackage = None
        if datapackage_bytes := await self.get_file("datapackage.json"):
            old_datapackage = DataPackage.model_validate_json(datapackage_bytes)

        # Create updated datapackage
        datapackage = DataPackage.from_filelist(
            self.data_source_id,
            [f for f in self.deposition.files if f.filename != "datapackage.json"],
            resources,
            self.deposition.metadata.version,
        )

        return datapackage, old_datapackage

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

    def get_existing_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        return list(self.deposition.files_map.keys())

    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        return self.deposition.links.html

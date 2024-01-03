"""Handle all deposition actions within Zenodo."""
import asyncio
import json
import logging
from typing import BinaryIO, Literal

import aiohttp
import semantic_version  # type: ignore  # noqa: PGH003

from pudl_archiver.utils import retry_async
from pudl_archiver.zenodo.entities import Deposition, DepositionMetadata, Record

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


class ZenodoDepositor:
    """Act on depositions & deposition files within Zenodo."""

    def __init__(
        self,
        upload_key: str,
        publish_key: str,
        session: aiohttp.ClientSession,
        sandbox: bool = True,
    ):
        """Create a new ZenodoDepositor.

        Args:
            upload_key: the Zenodo API key that gives you upload rights.
            publish_key: the Zenodo API key that gives you publish rights.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            sandbox: whether to hit the sandbox Zenodo instance or the real one. Default True.
        """
        self.auth_write = {"Authorization": f"Bearer {upload_key}"}
        self.auth_actions = {"Authorization": f"Bearer {publish_key}"}
        self.request = self._make_requester(session)

        self.sandbox = sandbox

        if sandbox:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

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

    async def publish_deposition(self, deposition: Deposition) -> Deposition:
        """Publish a deposition.

        Needs to have at least one file, and needs to not already be published.

        Args:
            deposition: the deposition you want to publish.
        """
        url = deposition.links.publish
        headers = {
            "Content-Type": "application/json",
        } | self.auth_actions
        response = await self.request(
            "POST", url, log_label="Publishing deposition", headers=headers
        )
        return Deposition(**response)

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

    async def delete_file(self, deposition: Deposition, target: str) -> None:
        """Delete a file from a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        candidates = [f for f in deposition.files if f.filename == target]
        if len(candidates) > 1:
            raise RuntimeError(
                f"More than one file matches the name {target}: {candidates}"
            )
        if len(candidates) == 0:
            logger.info(f"No files matched {target}.")
            return None

        response = await self.request(
            "DELETE",
            candidates[0].links.self,
            parse_json=False,
            log_label=f"Deleting {target} from deposition {deposition.id_}",
            headers=self.auth_write,
        )
        return response

    async def create_file(
        self,
        deposition: Deposition,
        target: str,
        data: BinaryIO,
        force_api: Literal["bucket", "files"] | None = None,
    ) -> None:
        """Create a file in a deposition.

        Attempts to use the new "bucket" API over the "files" API, but you can
        force it to use "files" if desired.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.
            force_api: force using one files API over another. The options are
                "bucket" and "files"

        Returns:
            None if success.
        """
        if deposition.links.bucket and force_api != "files":
            url = f"{deposition.links.bucket}/{target}"
            return await self.request(
                "PUT",
                url,
                log_label=f"Uploading {target} to bucket",
                data=data,
                headers=self.auth_write,
                timeout=3600,
            )
        if deposition.links.files and force_api != "bucket":
            url = f"{deposition.links.files}"
            return await self.request(
                "POST",
                url,
                log_label=f"Uploading {target} to files API",
                data={"file": data, "name": target},
                headers=self.auth_write,
            )
        raise RuntimeError("No file or bucket link available for deposition.")

    async def update_file(
        self, deposition: Deposition, target: str, data: BinaryIO
    ) -> None:
        """Update a file in a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        await self.delete_file(deposition, target)
        return await self.create_file(deposition, target, data)

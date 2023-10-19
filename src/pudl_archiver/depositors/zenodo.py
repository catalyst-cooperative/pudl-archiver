"""Handle all deposition actions within Zenodo."""
import asyncio
import json
import logging
from typing import BinaryIO, Literal

import aiohttp
import semantic_version  # type: ignore

from pudl_archiver.utils import retry_async
from pudl_archiver.zenodo.entities import Deposition, DepositionMetadata

logger = logging.getLogger(f"catalystcoop.{__name__}")


class ZenodoClientException(Exception):
    """Captures the JSON error information from Zenodo."""

    def __init__(self, kwargs):
        """Constructor.

        Args:
            kwargs: dictionary with "response" mapping to the actual
                aiohttp.ClientResponse and "json" mapping to the JSON content.
        """
        self.kwargs = kwargs
        self.status = kwargs["response"].status
        self.message = kwargs["json"].get("message", {})
        self.errors = kwargs["json"].get("errors", {})

    def __str__(self):
        """The JSON has all we really care about."""
        return f"ZenodoClientException({self.kwargs['json']})"

    def __repr__(self):
        """But the kwargs are useful for recreating this object."""
        return f"ZenodoClientException({repr(self.kwargs)})"


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
                response = await session.request(method, url, **kwargs)
                if response.status >= 400:
                    raise ZenodoClientException(
                        {"response": response, "json": await response.json()}
                    )
                if parse_json:
                    return await response.json()
                return response

            response = await retry_async(
                run_request,
                retry_on=(
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    ZenodoClientException,
                ),
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
        url = f"{self.api_root}/records"
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
        url = f"{self.api_root}/records"
        params = {"q": f'conceptdoi:"{concept_doi}"'}

        response = await self.request(
            "GET",
            url,
            f"Query depositions for {concept_doi}",
            params=params,
            headers=self.auth_write,
        )
        if len(response) > 1:
            logger.info(
                f"{concept_doi} points at multiple records: {[r['id'] for r in response]}"
            )
        depositions = [Deposition(**dep) for dep in response]

        if published_only:
            depositions = [dep for dep in depositions if dep.submitted]

        latest_deposition = sorted(depositions, key=lambda d: d.id_)[-1]
        return await self.get_record(latest_deposition.id_)

    async def get_record(self, rec_id: int) -> Deposition:
        """Get a deposition by its record ID directly instead of through concept.

        Args:
            rec_id: The record ID of the deposition you would like to get.
        """
        response = await self.request(
            "GET",
            f"{self.api_root}/records/{rec_id}",
            log_label=f"Get deposition for {rec_id}",
            headers=self.auth_write,
        )
        return Deposition(**response)

    async def get_new_version(
        self, deposition: Deposition, clobber: bool = False
    ) -> Deposition:
        """Get a new version of a deposition.

        First creates a new version, which is a snapshot of the old one, then
        updates that version with a new version number.

        Args:
            deposition: the deposition you want to get the new version of.
            clobber: if there is an existing draft, delete it and get a new one.

        Returns:
            A new Deposition that is a snapshot of the old one you passed in,
            with a new major version number.
        """
        if not deposition.submitted:
            if clobber:
                await self.delete_deposition(deposition)
                deposition = await self.get_deposition(
                    str(deposition.conceptdoi), published_only=True
                )
            else:
                return deposition

        new_deposition = await self.get_new_deposition_version(deposition)
        # If files already in new deposition, remove.
        if len(new_deposition.files) > 0:
            await self.delete_deposition(new_deposition)  # DEBUG ME!
            new_deposition = await self.get_new_deposition_version(
                deposition
            )  # Debug: Something isn't working here!

        # Link files from previous deposition.
        await self.link_previous_files(new_deposition)

        # Update metadata from previous deposition.
        old_metadata = deposition.metadata.dict(by_alias=True)
        new_metadata = new_deposition.metadata.dict(by_alias=True)

        # Update version number from previous metadata.
        previous = semantic_version.Version(old_metadata["version"])
        version_info = previous.next_major()

        new_metadata["version"] = str(version_info)
        # TO DO - update DOI field??
        # To do - fix pydantic validation errors for contributors and license.

        # Update metadata of new deposition with new version info
        data = json.dumps({"metadata": new_metadata})

        # Get url to newest deposition
        new_deposition_url = new_deposition.links.self
        logger.info(new_deposition_url)
        headers = {
            "Content-Type": "application/json",
        } | self.auth_write

        response = await self.request(
            "PUT",
            new_deposition_url,
            log_label=f"Updating version number from {previous} ({deposition.id_}) to {version_info}",
            data=data,
            headers=headers,
        )
        logger.info(response)
        return Deposition(**response)

    async def get_new_deposition_version(self, deposition: Deposition):
        """Create a new draft deposition version."""
        url = f"{self.api_root}/records/{deposition.id_}/versions"
        response = await self.request(
            "POST",
            url,
            log_label="Creating new version.",
            headers=self.auth_write,
        )
        return Deposition(**response)

    async def link_previous_files(self, new_deposition: Deposition) -> Deposition:
        """Transfer files over to new deposition."""
        draft_id = new_deposition.record_id
        url = f"{self.api_root}/records/{draft_id}/draft/actions/files-import"
        await self.request(
            "POST",
            url,
            log_label="Linking files from previous deposition.",
            headers=self.auth_write,
        )

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

        Args:
            deposition: the deposition you want to delete.
        """
        await self.request(
            "DELETE",
            deposition.links.self,
            log_label="Deleting deposition",
            headers=self.auth_write,
            parse_json=False,
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
    ) -> None:
        """Create a file in a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        response = await self.start_file_upload(deposition, target)
        file_links = response.json()["entries"][0]["links"]
        await self.upload_data_to_file(file_links, target, data)
        return await self.complete_file_upload(file_links, target)

    async def start_file_upload(
        self,
        deposition: Deposition,
        target: str,
    ) -> None:
        """Start a file deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        headers = {
            "Content-Type": "application/json",
        } | self.auth_write
        data = json.dumps([{"key": target}])
        return await self.request(
            "POST",
            deposition.links.files,
            log_label=f"Starting upload of {target}",
            headers=headers,
            data=data,
        )

    async def upload_data_to_file(
        self, file_links: dict, target: str, data: BinaryIO
    ) -> None:
        """Upload data to a file deposition.

        Args:
            file_links: list of Zenodo links associated with file.
            target: the filename of the file you want to upload data to.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        headers = {
            "Content-Type": "application/octet-stream",
        } | self.auth_write
        return await self.request(
            "PUT",
            file_links["content"],
            log_label=f"Uploading {target} data",
            data={"content": data},
            headers=headers,
        )

    async def complete_file_upload(
        self,
        file_links: dict,
        target: str,
    ) -> None:
        """Create a file in a deposition.

        Args:
            file_links: list of Zenodo links associated with file.
            target: the filename of the file you want to upload.

        Returns:
            None if success.
        """
        return await self.request(
            "POST",
            file_links["commit"],
            log_label=f"Committing upload of {target}",
            headers=self.auth_write,
        )

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

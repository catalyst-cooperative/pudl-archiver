"""Handle all deposition actions within Zenodo."""
import json
import logging
from typing import BinaryIO

import aiohttp
import semantic_version

from pudl_archiver.zenodo.entities import Deposition, DepositionMetadata

logger = logging.getLogger(f"catalystcoop.{__name__}")


# TODO (daz): start using mypy on this file.
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
    """Deposition actions within Zenodo.

    * manipulate depositions & their versions
    * manipulate files within depositions
    """

    def __init__(
        self,
        upload_key: str,
        publish_key: str,
        session: aiohttp.ClientSession,
        sandbox: bool = True,
    ):
        """Constructor.

        Args:
            upload_key: the Zenodo API key that gives you upload rights.
            publish_key: the Zenodo API key that gives you publish rights.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            sandbox: whether to hit the sandbox Zenodo instance or the real one. Default True.
        """
        self.upload_key = upload_key
        self.publish_key = publish_key
        self.request = self._make_requester(session)

        self.sandbox = sandbox

        if sandbox:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

    def _make_requester(self, session):
        """Wraps our session requests with some Zenodo-specific error handling."""

        async def requester(
            method: str, url: str, log_label: str, parse_json: bool = True, **kwargs
        ):
            logger.info(f"{method} {url} - {log_label}")
            async with session.request(method, url, **kwargs) as response:
                if response.status >= 400:
                    raise ZenodoClientException(
                        {"response": response, "json": await response.json()}
                    )
                if parse_json:
                    return await response.json()
                return response

        return requester

    async def create_deposition(self, metadata: DepositionMetadata) -> Deposition:
        """Create a whole new deposition.

        Args:
            metadata: a metadata, to make a deposition with.
        """
        url = f"{self.api_root}/deposit/depositions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"bearer {self.upload_key}",
        }

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

    # TODO (daz): this is more "query" deposition than "get" deposition, right?
    async def get_deposition(self, concept_doi: str) -> Deposition:
        """Get a deposition from a concept DOI.

        Args:
            concept_doi: the DOI for the concept - gets the latest associated DOI.
        """
        url = f"{self.api_root}/deposit/depositions"
        params = {"q": f'conceptdoi:"{concept_doi}"'}
        headers = {
            "Authorization": f"bearer {self.upload_key}",
        }

        response = await self.request(
            "GET",
            url,
            f"Query depositions for {concept_doi}",
            params=params,
            headers=headers,
        )
        if len(response) > 1 and not self.sandbox:
            # TODO (daz): not convinced we can't just always pick the most recent one.
            raise RuntimeError("Zenodo should only return a single deposition")

        latest_deposition = Deposition(**sorted(response, key=lambda d: d["id"])[-1])
        full_deposition_json = await self.request(
            "GET",
            f"{url}/{latest_deposition.id_}",
            log_label=f"Get freshest data for {latest_deposition.id_}",
            headers=headers,
        )
        return Deposition(**full_deposition_json)

    async def get_new_version(self, deposition: Deposition) -> Deposition:
        """Get a new version of a deposition.

        Args:
            deposition: the deposition you want to get the new version of.

        Returns:
            A new Deposition that is a snapshot of the old one you passed in,
            with a new major version number.
        """
        if not deposition.submitted:
            return deposition

        url = f"{self.api_root}/deposit/depositions/{deposition.id_}/actions/newversion"

        # Create the new version
        headers = {
            "Authorization": f"bearer {self.upload_key}",
        }

        # When the API creates a new version, it does not return the new one.
        # It returns the old one with a link to the new one.
        response = await self.request(
            "POST", url, log_label="Creating new version", headers=headers
        )
        old_deposition = Deposition(**response)

        source_metadata = old_deposition.metadata.dict(by_alias=True)
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
        new_deposition_url = old_deposition.links.latest_draft
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"bearer {self.upload_key}",
        }

        response = await self.request(
            "PUT",
            new_deposition_url,
            log_label=f"Updating version number from {previous} ({old_deposition.id_}) to {version_info}",
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
            "Authorization": f"bearer {self.publish_key}",
            "Content-Type": "application/json",
        }
        response = await self.request(
            "POST", url, log_label="Publishing deposition", headers=headers
        )
        return Deposition(**response)

    async def discard_deposition(self, deposition: Deposition) -> None:
        """Discard a deposition.

        This deposition must either be un-published or in the editing state.

        Args:
            deposition: the deposition you want to discard.

        Returns:
            None if success.
        """
        url = deposition.links.discard
        headers = {
            "Authorization": f"bearer {self.publish_key}",
            "Content-Type": "application/json",
        }
        response = await self.request(
            "POST", url, log_label="Discarding deposition", headers=headers
        )
        return Deposition(**response)

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
            headers={"Authorization": f"bearer {self.upload_key}"},
        )
        return response

    async def create_file(
        self,
        deposition: Deposition,
        target: str,
        data: BinaryIO,
        force_api: str | None = None,
    ) -> None:
        """Create a file in a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.
            force_api: force using one files API over another. The options are
                "bucket" and "files"

        Returns:
            None if success.
        """
        headers = {"Authorization": f"bearer {self.upload_key}"}
        if deposition.links.bucket and force_api != "files":
            url = f"{deposition.links.bucket}/{target}"
            return await self.request(
                "PUT",
                url,
                log_label=f"Uploading {target} to bucket",
                data=data,
                headers=headers,
            )
        elif deposition.links.files and force_api != "bucket":
            url = f"{deposition.links.files}"
            return await self.request(
                "POST",
                url,
                log_label=f"Uploading {target} to files API",
                data={"file": data, "name": target},
                headers=headers,
            )
        else:
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

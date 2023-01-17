"""Handle all deposition actions within Zenodo."""
# TODO (daz): fix flake8 so it stops asking for so many repetitive docstrings.
import logging
from typing import IO

import aiohttp

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

        async def requester(method, url, log_label, **kwargs):
            logger.info(f"{method} {url} - {log_label}")
            async with session.request(method, url, **kwargs) as response:
                resp_json = await response.json()
                if response.status >= 400:
                    raise ZenodoClientException(
                        {"response": response, "json": resp_json}
                    )
                return resp_json

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
            "Query depositions for {concept_doi}",
            params=params,
            headers=headers,
        )
        if len(response) > 1 and not self.sandbox:
            # TODO (daz): not convinced we can't just always pick the most recent one.
            raise RuntimeError("Zenodo should only return a single deposition")

        latest_deposition = sorted(response, key=lambda d: d["id"])[-1]
        return Deposition(**latest_deposition)

    def get_new_version(self, deposition: Deposition) -> Deposition:
        """Get a new version of a deposition.

        Args:
            deposition: the deposition you want to get the new version of.
        """
        raise NotImplementedError

    def publish_deposition(self, deposition: Deposition) -> Deposition:
        """Publish a deposition.

        Needs to have at least one file, and needs to not already be published.

        Args:
            deposition: the deposition you want to publish.
        """
        raise NotImplementedError

    # TODO (daz): should we return a success/failure flag instead of None/error?
    def discard_deposition(self, deposition: Deposition) -> None:
        """Discard a deposition.

        This deposition must either be un-published or in the editing state.

        Args:
            deposition: the deposition you want to discard.

        Returns:
            None if success.
        """
        raise NotImplementedError

    def delete_file(self, deposition: Deposition, target: str) -> None:
        """Delete a file from a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        raise NotImplementedError

    # TODO (daz): is data actually type BinaryIO?
    def create_file(self, deposition: Deposition, target: str, data: IO) -> None:
        """Create a file in a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        raise NotImplementedError

    def update_file(self, deposition: Deposition, target: str, data: IO) -> None:
        """Update a file in a deposition.

        Args:
            deposition: the deposition you are applying this change to.
            target: the filename of the file you want to delete.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        self.delete_file(deposition, target)
        self.create_file(deposition, target, data)

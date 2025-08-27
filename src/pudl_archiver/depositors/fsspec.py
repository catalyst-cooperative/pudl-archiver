"""Depositor implementation using any fsspec compatible file-system as storage backend.

This Depositor can't rely on Zenodo's built-in draft deposition/publication functionality,
so it implements this functionality itself. When using the Zenodo depositor, Zenodo will
automatically create a new draft where we can make changes to an archive without actually
overwritting the previous version. Only after intentionally publishing the draft do those
changes actually overwrite the official version of the archive.

To mimic this behavior, the fsspec Depositor will create two subdirectories within the
configured ``deposition_path``. One of these directories is used to store a working draft,
while the other is used to store official 'published' data. During an archiver run, files
will be uploaded to the working draft, and only after we call the ``publish`` method will
those files be moved to the ``published`` directory. If we detect any errors during a run,
like a poorly formatted file, then we won't call ``publish``, but the files will still
exist in the ``draft`` directory for inspection.

One piece of Zenodo functionality that this Depositor does not implement is versioning. In
Zenodo, when we publish a new deposition, the old version will still exist with a distinct
DOI that we can use to point to that version. At this point, the fsspec Depositor will just
overwrite data in the published directory, so the old version will disappear.
"""

import base64
import logging
import traceback
from enum import Enum
from typing import BinaryIO

import aiohttp
from pydantic import ConfigDict
from upath import UPath

from pudl_archiver.depositors.depositor import (
    DepositionAction,
    DepositionChange,
    DepositionState,
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
from pudl_archiver.utils import RunSettings, compute_md5

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _resource_from_upath(path: UPath, parts: Partitions, md5_hash: str) -> Resource:
    """Create a resource from a single file with partitions.

    Args:
        path: UPath pointing to resource on local or remote filesystem.
        parts: Working partitions of current resource.
        md5_hash: String md5 hash of resource.
    """
    mt = MEDIA_TYPES[path.suffix[1:]]

    return Resource(
        name=path.name,
        path=path.as_uri().replace("draft", "published"),
        remote_url=path.as_uri().replace("draft", "published"),
        title=path.name,
        mediatype=mt,
        parts=parts,
        bytes=path.stat().st_size,
        hash=md5_hash,
        format=path.suffix,
    )


class DepositionStatus(Enum):
    """Enum representing the state of a deposition, which can be either published or a draft."""

    PUBLISHED = "published"
    DRAFT = "draft"


class Deposition(DepositionState):
    """Represent an fsspec deposition."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    deposition_path: UPath
    #: static list of files from deposition prior to any modifications
    deposition_files: dict[DepositionStatus, list[str]]
    version: str = "0.1"

    def get_deposition_path(self, deposition_state: DepositionStatus) -> UPath:
        """Return path to draft or published version of deposition."""
        return self.deposition_path / deposition_state.value

    def get_checksum(self, filepath: UPath) -> str | None:
        """Get checksum for a file in the current deposition.

        Args:
            filepath: Path of file to checksum.
        """
        md5_hash = None
        if filepath.exists():
            # For Google Cloud fsspec backend we can use the `info` method to get
            # The md5 hash without having to read the entire file we just uploaded
            if filepath.protocol == "gs":
                md5_hash = base64.urlsafe_b64decode(
                    filepath.fs.info(filepath.as_uri(), detail=True)["md5Hash"]
                ).hex()
            # Not all filesystems return an md5 hash from `info` method, so default
            # to manual computation
            else:
                md5_hash = compute_md5(filepath)
        return md5_hash

    @classmethod
    def from_upath(cls, deposition_path: UPath):
        """Construct deposition object from fsspec path to deposition."""
        deposition_files = {
            DepositionStatus.PUBLISHED: [],
            DepositionStatus.DRAFT: [],
        }

        # Loop published/draft deposition and find existing files
        for deposition_state in DepositionStatus:
            if (deposition_path / deposition_state.value).exists():
                deposition_files[deposition_state] = [
                    str(child.name)
                    for child in (deposition_path / deposition_state.value).iterdir()
                    if child.name != (deposition_path / deposition_state.value).name
                ]
        # Return constructed deposition
        return cls(
            deposition_path=deposition_path.absolute(),
            deposition_files=deposition_files,
        )


class FsspecAPIClient(DepositorAPIClient):
    """Implement API for fsspec based depositors."""

    path: str

    @classmethod
    async def initialize_client(
        cls,
        session: aiohttp.ClientSession,
        sandbox: bool,
        deposition_path: str,
    ) -> "FsspecAPIClient":
        """Return initialized fsspec api client."""
        logger.warning(
            "The fsspec depositor backend is in an early/experimental state. "
            "It currently does not support versioning so any existing archive will be overwritten "
            "after publishing. Please use with caution."
        )
        if sandbox:
            raise NotImplementedError(
                "There is no sandbox available for fsspec archiver. "
                "An alternative for testing would be to use the 'local' depositor backend."
            )
        return cls(path=deposition_path)

    async def get_deposition(self, dataset_id: str):
        """Get latest version of deposition associated with dataset_id."""
        return Deposition.from_upath(
            deposition_path=UPath(self.path),
        )

    async def create_new_deposition(self, dataset_id: str):
        """Prepare new deposition associated with dataset_id."""
        deposition_path = UPath(self.path)
        deposition_path.mkdir(parents=True, exist_ok=True)

        return Deposition.from_upath(deposition_path=deposition_path)

    def get_file(
        self, deposition: Deposition, filename: str, deposition_state: DepositionStatus
    ) -> bytes:
        """Download file from deposition."""
        with (deposition.get_deposition_path(deposition_state) / filename).open(
            "rb"
        ) as f:
            return f.read()


class FsspecPublishedDeposition(PublishedDeposition):
    """Represents published version of fsspec deposition."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    settings: RunSettings
    deposition: Deposition
    api_client: FsspecAPIClient
    dataset_id: str

    async def list_files(self):
        """List files."""
        return self.deposition.deposition_files[DepositionStatus.PUBLISHED]

    def get_deposition_link(self) -> str:
        """Return link to deposition."""
        return self.deposition.deposition_path.as_uri()

    async def get_file(self, filename: str) -> bytes:
        """Download file from deposition."""
        return self.api_client.get_file(
            self.deposition, filename, deposition_state=DepositionStatus.PUBLISHED
        )

    async def open_draft(self) -> "FsspecDraftDeposition":
        """Open a new draft to make edits."""
        return FsspecDraftDeposition(
            deposition=self.deposition,
            settings=self.settings,
            api_client=self.api_client,
            dataset_id=self.dataset_id,
            # When we open a new draft we assume it starts with all files from previous version
            resources_in_draft={
                fname: self.deposition.get_deposition_path(DepositionStatus.PUBLISHED)
                / fname
                for fname in await self.list_files()
            },
        )


class FsspecDraftDeposition(DraftDeposition):
    """Represents draft version of fsspec deposition."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    settings: RunSettings
    deposition: Deposition
    api_client: FsspecAPIClient
    dataset_id: str
    resources_in_draft: dict[str, UPath] = {}
    files_to_delete: dict[str, UPath] = {}

    async def list_files(self):
        """Return the union of files from the current draft and previous published version."""
        # Check for files that are in the draft directory but not in resources_in_draft
        # This is expected behavior if we are re-running a failed run
        if missing_files := set(
            self.deposition.deposition_files[DepositionStatus.DRAFT]
        ) - set(self.resources_in_draft.keys()):
            self.resources_in_draft |= {
                fname: self.deposition.get_deposition_path(DepositionStatus.DRAFT)
                / fname
                for fname in missing_files
            }

        return list(self.resources_in_draft.keys())

    def get_deposition_link(self) -> str:
        """Return link to deposition."""
        return self.deposition.deposition_path.as_uri()

    async def publish(self) -> FsspecPublishedDeposition:
        """Publish deposition."""
        # Delete files no longer included in published deposition
        self.deposition.get_deposition_path(DepositionStatus.PUBLISHED).mkdir(
            exist_ok=True
        )
        for path in self.files_to_delete.values():
            path.unlink()

        # Move files from draft to published deposition
        for filename in self.deposition.deposition_files[DepositionStatus.DRAFT]:
            (
                self.deposition.get_deposition_path(DepositionStatus.DRAFT) / filename
            ).rename(
                self.deposition.get_deposition_path(DepositionStatus.PUBLISHED)
                / filename
            )

        return FsspecPublishedDeposition(
            dataset_id=self.dataset_id,
            api_client=self.api_client,
            settings=self.settings.model_copy(update={"initialize": False}),
            deposition=Deposition.from_upath(self.deposition.deposition_path),
        )

    def get_checksum(self, filename: str) -> str | None:
        """Get checksum for a file in the current deposition.

        Args:
            filename: Name of file to checksum.
        """
        checksum = None
        if filepath := self.resources_in_draft.get(filename):
            checksum = self.deposition.get_checksum(filepath)
        return checksum

    async def create_file(
        self,
        filename: str,
        data: BinaryIO,
    ) -> "FsspecDraftDeposition":
        """Create a file in a deposition."""
        self.deposition.get_deposition_path(DepositionStatus.DRAFT).mkdir(exist_ok=True)
        new_file_path = (
            self.deposition.get_deposition_path(DepositionStatus.DRAFT) / filename
        )
        with new_file_path.open(mode="wb") as f:
            f.write(data.read())

        return self.model_copy(
            update={
                "deposition": Deposition.from_upath(self.deposition.deposition_path),
                "resources_in_draft": self.resources_in_draft
                | {filename: new_file_path},
            }
        )

    async def delete_file(self, filename: str) -> "FsspecDraftDeposition":
        """Delete a file from a deposition."""
        return self.model_copy(
            update={
                "files_to_delete": self.files_to_delete
                | {filename: self.resources_in_draft[filename]},
                "resources_in_draft": {
                    key: value
                    for key, value in self.resources_in_draft.items()
                    if key != filename
                },
            }
        )

    async def get_file(self, filename: str) -> bytes:
        """Download file from deposition."""
        return self.api_client.get_file(
            self.deposition, filename, deposition_state=DepositionStatus.DRAFT
        )

    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        logger.error(
            f"Failed while creating new deposition: {traceback.print_exception(e)}"
        )

    async def delete_deposition(self) -> None:
        """Delete an un-submitted deposition."""
        raise NotImplementedError(
            "Versioning is not yet implemented for fsspec backend, so deleting a draft deposition is not possible."
        )

    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange:
        """Check whether file exists in most recent published version and should be deleted."""
        remote_path = (
            self.deposition.get_deposition_path(DepositionStatus.PUBLISHED) / filename
        )

        if remote_path.exists():
            remote_md5 = self.deposition.get_checksum(remote_path)
            local_md5 = compute_md5(resource.local_path)
            if remote_md5 != local_md5:
                logger.info(
                    f"Updating {filename}: local hash {local_md5} vs. remote {remote_md5}"
                )
                action = DepositionAction.UPDATE
            else:
                action = DepositionAction.NO_OP
        else:
            logger.info(f"Adding {filename} to deposition.")

            action = DepositionAction.CREATE

        return DepositionChange(
            action_type=action,
            name=filename,
            resource=resource.local_path,
        )

    def generate_datapackage(
        self,
        partitions_in_deposition: dict[str, Partitions],
    ) -> DataPackage:
        """Generate new datapackage, attach to deposition, and return."""
        logger.info(f"Creating new datapackage.json for {self.dataset_id}")

        # Create updated datapackage
        resources = [
            _resource_from_upath(
                path,
                partitions_in_deposition[fname],
                self.deposition.get_checksum(path),
            )
            for fname, path in self.resources_in_draft.items()
            if fname != "datapackage.json" and fname not in self.files_to_delete
        ]
        datapackage = DataPackage.new_datapackage(
            self.dataset_id,
            resources,
            self.deposition.version,
        )

        return datapackage


register_depositor(
    "fsspec", FsspecAPIClient, FsspecPublishedDeposition, FsspecDraftDeposition
)

"""Depositor implementation using any fsspec compatible file-system as storage backend."""

import base64
import logging
import traceback
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
        hash=compute_md5(path),
        format=path.suffix,
    )


class Deposition(DepositionState):
    """Represent an fsspec deposition."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    deposition_path: UPath
    #: static list of files from deposition prior to any modifications
    draft_file_list: list[str]
    published_file_list: list[str]
    version: str = "0.1"

    def get_deposition_path(self, deposition_state: str) -> UPath:
        """Return path to draft or published version of deposition."""
        return self.deposition_path / deposition_state

    def get_checksum(self, filename: str, deposition_state: str) -> str | None:
        """Get checksum for a file in the current deposition.

        Args:
            filename: Name of file to checksum.
            deposition_state: Specify whether to get checksum from published or draft deposition.
        """
        md5_hash = None
        filepath = self.get_deposition_path(deposition_state) / filename
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
        file_lists = {
            "draft_file_list": [],
            "published_file_list": [],
        }
        for deposition_state in ["draft", "published"]:
            if (deposition_path / deposition_state).exists():
                file_lists[f"{deposition_state}_file_list"] = [
                    str(child.name)
                    for child in (deposition_path / deposition_state).iterdir()
                    if child.name != (deposition_path / deposition_state).name
                ]
        return cls(
            deposition_path=deposition_path.absolute(),
            **file_lists,
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
            "It currently does not support versioning so any existing archive will be overwritten. "
            "Please use with caution."
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
        self, deposition: Deposition, filename: str, deposition_state: str
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
        return self.deposition.published_file_list

    def get_deposition_link(self) -> str:
        """Return link to deposition."""
        return self.deposition.deposition_path.as_uri()

    async def get_file(self, filename: str) -> bytes:
        """Download file from deposition."""
        return self.api_client.get_file(
            self.deposition, filename, deposition_state="published"
        )

    async def open_draft(self) -> "FsspecDraftDeposition":
        """Open a new draft to make edits."""
        return FsspecDraftDeposition(
            deposition=self.deposition,
            settings=self.settings,
            api_client=self.api_client,
            dataset_id=self.dataset_id,
        )


class FsspecDraftDeposition(DraftDeposition):
    """Represents draft version of fsspec deposition."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    settings: RunSettings
    deposition: Deposition
    api_client: FsspecAPIClient
    dataset_id: str
    files_to_delete: list[str] = []

    async def list_files(self):
        """Return the union of files from the current draft and previous published version."""
        return [path.name for path in self._list_paths()]

    def _list_paths(self):
        """Return the union of files from the current draft and previous published version."""
        draft_paths = [
            self.deposition.get_deposition_path("draft") / fname
            for fname in self.deposition.draft_file_list
        ]
        published_paths = [
            self.deposition.get_deposition_path("published") / fname
            for fname in self.deposition.published_file_list
            if fname not in self.files_to_delete
        ]
        all_paths = draft_paths + published_paths
        all_fnames = [path.name for path in all_paths]
        if len(all_fnames) != len(set(all_fnames)):
            raise RuntimeError(
                "fsspec depositor detected duplicate files in current draft."
            )
        return all_paths

    def get_deposition_link(self) -> str:
        """Return link to deposition."""
        return self.deposition.deposition_path.as_uri()

    async def publish(self) -> FsspecPublishedDeposition:
        """Publish deposition."""
        # Delete files no longer included in published deposition
        self.deposition.get_deposition_path("published").mkdir(exist_ok=True)
        for filename in self.files_to_delete:
            (self.deposition.get_deposition_path("published") / filename).unlink()

        # Move files from draft to published deposition
        for filename in self.deposition.draft_file_list:
            (self.deposition.get_deposition_path("draft") / filename).rename(
                self.deposition.get_deposition_path("published") / filename
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
        return self.deposition.get_checksum(filename, deposition_state="draft")

    async def create_file(
        self,
        filename: str,
        data: BinaryIO,
    ) -> "FsspecDraftDeposition":
        """Create a file in a deposition."""
        self.deposition.get_deposition_path("draft").mkdir(exist_ok=True)
        with (self.deposition.get_deposition_path("draft") / filename).open(
            mode="wb"
        ) as f:
            f.write(data.read())

        return self.model_copy(
            update={
                "deposition": Deposition.from_upath(self.deposition.deposition_path)
            }
        )

    async def delete_file(self, filename: str) -> "FsspecDraftDeposition":
        """Delete a file from a deposition."""
        return self.model_copy(
            update={
                "files_to_delete": self.files_to_delete + [filename],
            }
        )

    async def get_file(self, filename: str) -> bytes:
        """Download file from deposition."""
        return self.api_client.get_file(
            self.deposition, filename, deposition_state="draft"
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
        remote_path = self.deposition.get_deposition_path("published") / filename

        if remote_path.exists():
            remote_md5 = self.deposition.get_checksum(
                filename, deposition_state="published"
            )
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

    async def generate_datapackage(
        self,
        partitions_in_deposition: dict[str, Partitions],
    ) -> DataPackage:
        """Generate new datapackage, attach to deposition, and return."""
        logger.info(f"Creating new datapackage.json for {self.dataset_id}")

        # Create updated datapackage
        resources = [
            _resource_from_upath(
                path,
                partitions_in_deposition[path.name],
                self.get_checksum(path.name),
            )
            for path in self._list_paths()
            if path.name != "datapackage.json" and path.name not in self.files_to_delete
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

"""Implements generic interface for depositors."""

import io
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import BinaryIO

import aiohttp
from pydantic import BaseModel

from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import RunSettings, Url

logger = logging.getLogger(f"catalystcoop.{__name__}")


@dataclass
class _UploadSpec:
    """Defines an upload that will be done by ZenodoDepositionInterface."""

    source: io.IOBase | Path
    dest: str


class FileWrapper(io.BytesIO):
    """Minimal wrapper around BytesIO to override close method to work around aiohttp."""

    def __init__(self, content: bytes):
        """Call base class __init__."""
        super().__init__(content)

    def close(self):
        """Don't close file, so aiohttp can't unexpectedly close files."""
        pass

    def actually_close(self):
        """Actually close the file for internal use."""
        super().close()


class DepositionAction(Enum):
    """Enumerate types of changes which can be applied to deposition files."""

    CREATE = (auto(),)
    UPDATE = (auto(),)
    DELETE = (auto(),)
    NO_OP = (auto(),)


@dataclass
class DepositionChange:
    """Define a single change to a file in a deposition."""

    action_type: DepositionAction
    name: str
    resource: io.IOBase | Path | None = None


class PublishedDeposition(BaseModel, ABC):
    """Wrapper class to manage state of deposition and calling interface."""

    settings: RunSettings

    @classmethod
    @abstractmethod
    async def get_latest_version(
        cls,
        dataset: str,
        session: aiohttp.ClientSession,
        run_settings: RunSettings,
    ) -> "PublishedDeposition":
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            run_settings: Settings from CLI.
        """
        ...

    @abstractmethod
    async def open_draft(self) -> "DraftDeposition":
        """Open a new draft deposition to make edits."""
        ...

    @abstractmethod
    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        ...

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        ...


class DraftDeposition(BaseModel, ABC):
    """Wrapper for a draft deposition which can be modified."""

    settings: RunSettings

    @abstractmethod
    async def publish(self) -> PublishedDeposition:
        """Publish draft deposition and return new depositor with updated deposition."""
        ...

    @abstractmethod
    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        ...

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        ...

    @abstractmethod
    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        ...

    @abstractmethod
    async def create_file(
        self,
        filename: str,
        data: BinaryIO,
    ):
        """Create a file in a deposition.

        Args:
            target: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        ...

    @abstractmethod
    async def delete_file(
        self,
        filename: str,
    ):
        """Delete a file from a deposition.

        Args:
            target: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        ...

    @abstractmethod
    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange | None:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        ...

    @abstractmethod
    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        ...

    @abstractmethod
    def generate_datapackage(self, resources: dict[str, ResourceInfo]) -> DataPackage:
        """Generate new datapackage and return it."""
        ...

    async def add_resource(self, name: str, resource: ResourceInfo):
        """Apply correct change to deposition based on downloaded resource."""
        change = self.generate_change(name, resource)
        await self._apply_change(change)

    async def publish_if_valid(
        self, run_summary: RunSummary
    ) -> PublishedDeposition | None:
        """Check that deposition is valid and worth changing, then publish if so."""
        logger.info("Attempting to publish deposition.")
        if len(run_summary.file_changes) == 0:
            logger.info(
                "No changes detected, kept draft at"
                f"{self.deposition.get_deposition_link()} for inspection."
            )
            return None
        return await self.deposition.publish()

    async def _apply_change(self, change: DepositionChange) -> None:
        """Actually upload and delete what we listed in self.uploads/deletes.

        Args:
            draft: the draft to make these changes to
            change: the change to make
        """
        if self.settings.dry_run:
            logger.info(f"Dry run, skipping {change}")
            return

        if change.action_type in [DepositionAction.DELETE, DepositionAction.UPDATE]:
            await self.delete_file(change.name)
        if change.action_type in [DepositionAction.CREATE, DepositionAction.UPDATE]:
            if change.resource is None:
                raise RuntimeError("Must pass a resource to be uploaded.")

            await self._upload_file(
                _UploadSpec(source=change.resource, dest=change.name)
            )

    async def _upload_file(self, upload: _UploadSpec):
        if isinstance(upload.source, io.IOBase):
            wrapped_file = FileWrapper(upload.source.read())
        else:
            with upload.source.open("rb") as f:
                wrapped_file = FileWrapper(f.read())

        await self.create_file(upload.dest, wrapped_file)

        wrapped_file.actually_close()

    def _datapackage_worth_changing(
        self, old_datapackage: DataPackage | None, new_datapackage: DataPackage
    ) -> bool:
        # ignore differences in created/version
        # ignore differences resource paths if it's just some ID number changing...
        if old_datapackage is None:
            return True
        for field in new_datapackage.model_dump():
            if field in {"created", "version"}:
                continue
            if field == "resources":
                for r in old_datapackage.resources + new_datapackage.resources:
                    r.path = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.path))
                    r.remote_url = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.remote_url))
            if getattr(new_datapackage, field) != getattr(old_datapackage, field):
                return True
        return False

    async def attach_datapackage(
        self,
        resources: dict[str, ResourceInfo],
        old_datapackage: DataPackage,
    ) -> DataPackage:
        """Generate new datapackage describing draft deposition in current state."""
        new_datapackage = self.generate_datapackage(resources)

        # Add datapackage if it's changed
        if self._datapackage_worth_changing(old_datapackage, new_datapackage):
            datapackage_json = io.BytesIO(
                bytes(
                    new_datapackage.model_dump_json(by_alias=True, indent=4),
                    encoding="utf-8",
                )
            )
            await self.create_file("datapackage.json", datapackage_json)
        return new_datapackage

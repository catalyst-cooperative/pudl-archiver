"""Implements generic interface for depositors."""

import io
import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import BinaryIO

import aiohttp
from pydantic import BaseModel, ConfigDict, PrivateAttr

from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import RunSettings, Url

logger = logging.getLogger(f"catalystcoop.{__name__}")


class _UploadSpec(BaseModel):
    """Defines an upload that will be done by ZenodoDepositionInterface."""

    source: io.IOBase | Path
    dest: str
    model_config = ConfigDict(arbitrary_types_allowed=True)


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


@dataclass
class DepositionChange:
    """Define a single change to a file in a deposition."""

    action_type: DepositionAction
    name: str
    resource: io.IOBase | Path | None = None


class AbstractDepositorInterface(ABC, BaseModel):
    """Abstract class defines read interface for depositor."""

    @classmethod
    @abstractmethod
    async def get_latest_version(
        cls,
        dataset: str,
        session: aiohttp.ClientSession,
        run_settings: RunSettings,
    ) -> "AbstractDepositorInterface":
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            run_settings: Settings from CLI.
        """
        ...

    @abstractmethod
    async def open_draft(self, create_new: bool) -> "AbstractDepositorInterface":
        """Open a new draft deposition to make edits."""
        ...

    @abstractmethod
    async def publish(self) -> "AbstractDepositorInterface":
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
    def generate_datapackage(self, resources: dict[str, ResourceInfo]) -> DataPackage:
        """Generate new datapackage, attach to deposition, and return."""
        ...


class DraftDeposition(BaseModel):
    """Wrapper for a draft deposition which can be modified."""

    deposition: AbstractDepositorInterface
    _new_resources: dict[str, ResourceInfo] = PrivateAttr(default=[])
    _run_valid: bool = PrivateAttr(default=False)

    def add_resource(self, name: str, resource: ResourceInfo):
        """Apply correct change to deposition based on downloaded resource."""
        self._new_resources[name] = resource
        change = self.deposition.generate_change(name, resource)
        self._apply_change(change)

    async def _apply_change(self, change: DepositionChange) -> None:
        """Actually upload and delete what we listed in self.uploads/deletes.

        Args:
            draft: the draft to make these changes to
            change: the change to make
        """
        self.changes.append(change)
        if self.dry_run:
            logger.info(f"Dry run, skipping {change}")
            return

        if change.action_type in [DepositionAction.DELETE, DepositionAction.UPDATE]:
            self.deposition = await self.deposition.delete_file(change.name)
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

        self.deposition = await self.deposition.create_file(upload.dest, wrapped_file)

        wrapped_file.actually_close()

    async def generate_datapackage(self):
        """Add datapckage to deposition."""
        await self.deposition.generate_datapackage(self._new_resources)

    def validate_run(self, run_summary: RunSummary):
        """Draft will only be published if passed a successful run summary while open."""
        self._run_valid = run_summary.success

    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        self.deposition.cleanup_after_error(e)

    async def publish(self):
        """Cleanup draft after an error during an archive run."""
        if self._run_valid:
            await self.deposition.publish()
        else:
            logger.error(
                "Archive validation failed. Not publishing new archive, kept "
                f"draft at {self.depositor.get_deposition_link()} for inspection."
            )


class Depositor(BaseModel):
    """Wrapper class to manage state of deposition and calling interface."""

    deposition: AbstractDepositorInterface
    settings: RunSettings

    @classmethod
    async def get_latest_version(
        cls,
        interface: type[AbstractDepositorInterface],
        dataset: str,
        session: aiohttp.ClientSession,
        settings: RunSettings,
    ) -> "Depositor":
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            dataset_settings_path: Path to settings file for each dataset.
            sandbox: whether to hit the sandbox Zenodo instance or the real one. Default True.
            create_new: whether or not we are adding a new dataset.
            resume_run: Attempt to resume a run that was previously interrupted.
            refresh_metadata: Regenerate metadata from PUDL data source rather than
                existing archived metadata.
        """
        interface = await interface.get_latest_version(
            dataset,
            session,
            sandbox=settings.sandbox,
            create_new=settings.initialize,
            resume_run=settings.resume_run,
            refresh_metadata=settings.refresh_metadata,
        )
        return cls(
            interface=interface,
            settings=settings,
        )

    @asynccontextmanager
    async def open_draft(self):
        """Context manager to open a draft deposition and cleanly handle closing draft."""
        draft_deposition = DraftDeposition(
            deposition=await self.deposition.open_draft(self.settings.initialize)
        )
        try:
            yield draft_deposition
        except Exception as e:
            await draft_deposition.cleanup_after_error(e)
        finally:
            await draft_deposition.publish()

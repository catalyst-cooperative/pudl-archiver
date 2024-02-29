"""Implements generic interface for depositors."""

import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import BinaryIO

import aiohttp

from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import Url


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


class AbstractDepositor(ABC):
    """Abstract class defines interface for all Depositors."""

    @abstractmethod
    def __init__(
        self,
        dataset: str,
        session: aiohttp.ClientSession,
        dataset_settings_path: Path,
        sandbox: bool,
    ):
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            dataset_settings_path: Path to settings file for each dataset.
            sandbox: whether to hit the sandbox Zenodo instance or the real one. Default True.
        """
        ...

    @abstractmethod
    async def prepare_depositor(
        self,
        create_new: bool,
        resume_run: bool,
        refresh_metadata: bool,
    ) -> dict[str, ResourceInfo]:
        """Perform any async setup necessary for depositor.

        Args:
            create_new: whether or not we are adding a new dataset.
            resume_run: Attempt to resume a run that was previously interrupted.
            refresh_metadata: Regenerate metadata from PUDL data source rather than
                existing archived metadata.
        """
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

    async def update_file(
        self,
        filename: str,
        data: BinaryIO,
    ):
        """Update a file in deposition.

        Args:
            target: the filename of the file you want to update.
            data: new file data to replace old file with.

        Returns:
            None if success.
        """
        await self.delete_file(filename)
        return await self.create_file(filename, data)

    @abstractmethod
    def generate_change(
        self, name: str, resource: ResourceInfo
    ) -> DepositionChange | None:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        ...

    @abstractmethod
    def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        ...

    @abstractmethod
    def get_existing_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        ...

    @abstractmethod
    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        ...

    @abstractmethod
    async def update_datapackage(
        self,
        resources: dict[str, ResourceInfo],
    ) -> tuple[DataPackage, DataPackage | None]:
        """Create new frictionless datapackage for deposition.

        Args:
            resources: Dictionary mapping resources to ResourceInfo which is used to
                generate new datapackage - we need this for the partition information.

        Returns:
            new DataPackage, old DataPackage
        """
        ...

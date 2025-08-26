"""Implements generic interface for depositors."""

import io
import logging
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import BinaryIO

import aiohttp
from pydantic import BaseModel, ConfigDict

from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.frictionless import DataPackage, Partitions, ResourceInfo
from pudl_archiver.utils import RunSettings, Url, compute_md5

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


class DepositorAPIClient(BaseModel, ABC):
    """This class is used to implement an interface for a depositor.

    DepositorAPIClient's only have 2 required methods, which are used to retrieve
    state needed to initialize a PublishedDeposition or DraftDeposition. This class
    may take configuration for the API, but should not maintain any state about a
    deposition.
    """

    @classmethod
    @abstractmethod
    async def initialize_client(
        cls,
        session: aiohttp.ClientSession,
        sandbox: bool,
        deposition_path: str | None = None,
    ) -> "DepositorAPIClient":
        """Initialize API client connection.

        Args:
            session: HTTP handler - we don't use it directly, it's wrapped in self._request.
            sandbox: False for production archives.
            deposition_path: Some depositors take a configurable path.
        """
        ...

    @abstractmethod
    async def get_deposition(self, dataset_id: str) -> typing.Any:
        """Get latest version of deposition associated with dataset_id.

        Should return object representing depositon that will be used to initialize a
        PublishedDeposition.
        """
        ...

    @abstractmethod
    async def create_new_deposition(self, dataset_id: str) -> typing.Any:
        """Create new deposition from scratch associated with dataset_id.

        Should return object representing depositon that will be used to initialize a
        DraftDeposition.
        """
        ...


class DepositionState(BaseModel):
    """Base class to define deposition state for a depositor.

    This base class does not define anything as the state will depend entirely on
    the specific depositor.
    """


class PublishedDeposition(BaseModel, ABC):
    """Abstract base class defining the interface for a published deposition.

    Published depositions should be read only, and provide the method `open_draft`
    to create an editable draft deposition. There are several read methods defined
    for this class that are also required for the `DraftDeposition`. The
    implementation of these methods may be different for the draft/published versions,
    or they may be identical, in which case you should create a base class to implement
    these shared methods, which can be inherited by both the draft and published versions.
    For an example of this, see `src/pudl_archiver/depositors/zenodo/depositor.py`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    settings: RunSettings
    api_client: DepositorAPIClient
    dataset_id: str
    deposition: DepositionState

    @classmethod
    async def get_most_recent_version(
        cls,
        settings: RunSettings,
        api_client: DepositorAPIClient,
        dataset_id: str,
    ) -> "PublishedDeposition":
        """Get most recent version of a published deposition."""
        deposition = await api_client.get_deposition(dataset_id)

        return cls(
            dataset_id=dataset_id,
            settings=settings,
            api_client=api_client,
            deposition=deposition,
        )

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
        """Return list of filenames from published version of deposition."""
        ...

    @abstractmethod
    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        ...


class DraftDeposition(BaseModel, ABC):
    """Abstract base class defining the interface for a draft deposition.

    Draft depositions contain both read/write functionality. All write methods
    should return a new `DraftDeposition` class that reflects any changed state
    due to the edit. The `publish` method should return a `PublishedDeposition`
    class reflecting the new version of the archive. This base class also defines
    several high level methods that make use of the abstract interface that each
    subclass should define. These methods are used by `src/pudl_archiver/orchestrator.py`
    and should not be overriden except in exceptional circumstances.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    settings: RunSettings
    api_client: DepositorAPIClient
    dataset_id: str
    deposition: DepositionState

    @classmethod
    async def new_draft(
        cls,
        settings: RunSettings,
        api_client: DepositorAPIClient,
        dataset_id: str,
    ) -> "DraftDeposition":
        """Construct a new deposition from scratch."""
        deposition = await api_client.create_new_deposition(dataset_id)
        return cls(
            dataset_id=dataset_id,
            settings=settings,
            api_client=api_client,
            deposition=deposition,
        )

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
    def get_checksum(self, filename: str) -> str | None:
        """Get checksum for a file in the current deposition.

        Args:
            filename: Name of file to checksum.
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
    ) -> "DraftDeposition":
        """Create a file in a deposition.

        Args:
            target: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            DraftDeposition with the created file
        """
        ...

    @abstractmethod
    async def delete_file(
        self,
        filename: str,
    ) -> "DraftDeposition":
        """Delete a file from a deposition.

        Args:
            target: the filename of the file you want to delete.

        Returns:
            DraftDeposition with the deleted file
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
    async def delete_deposition(self):
        """Delete deposition if no changes found."""
        ...

    @abstractmethod
    async def generate_datapackage(
        self, partitions_in_deposition: dict[str, Partitions]
    ) -> DataPackage:
        """Generate new datapackage and return it."""
        ...

    async def add_resource(
        self, name: str, resource: ResourceInfo
    ) -> "DraftDeposition":
        """Apply correct change to deposition based on downloaded resource."""
        change = self.generate_change(name, resource)
        return await self._apply_change(change)

    async def publish_if_valid(
        self,
        run_summary: RunSummary,
        clobber_unchanged: bool,
        auto_publish: bool,
    ) -> PublishedDeposition | None:
        """Check that deposition is valid and worth changing, then publish if so."""
        if not run_summary.success:
            logger.error(
                "Archive validation failed. Not publishing new archive, kept "
                f"draft at {self.get_deposition_link()} for inspection."
            )
            return run_summary
        if len(run_summary.file_changes) == 0 and not run_summary.datapackage_changed:
            if clobber_unchanged:
                await self.delete_deposition()
                logger.info("No changes detected, deleted draft.")
            else:
                logger.info(
                    "No changes detected, kept draft at "
                    f"{self.get_deposition_link()} for inspection."
                )
            return None
        if not auto_publish:
            logger.info(
                "Skipping publishing because auto-publish is disabled, kept draft at "
                f"{self.get_deposition_link()} for inspection."
            )
            return None

        logger.info("Attempting to publish deposition.")
        return await self.publish()

    async def _apply_change(
        self, change: DepositionChange, checksum_retry_count: int = 7
    ) -> "DraftDeposition":
        """Actually upload and delete what we listed in self.uploads/deletes.

        Args:
            change: the change to make
            checksum_retry_count: how many times to try an upload again if the
                finished remote file doesn't match checksums with the local file;
                default 7
        """
        draft = self
        if change.action_type in [DepositionAction.DELETE, DepositionAction.UPDATE]:
            draft = await self.delete_file(change.name)
        if change.action_type in [DepositionAction.CREATE, DepositionAction.UPDATE]:
            if change.resource is None:
                raise RuntimeError("Must pass a resource to be uploaded.")

            checksum = compute_md5(change.resource)
            for chance in range(checksum_retry_count):
                draft = await self._upload_file(
                    _UploadSpec(source=change.resource, dest=change.name)
                )
                if draft.get_checksum(change.name) == checksum:
                    break
                logger.warning(
                    f"Upload of {change.name} failed with nonmatching checksum (try {chance + 1} of {checksum_retry_count})"
                )
                # drop the bad upload before retrying
                draft = await self.delete_file(change.name)
            else:  # if we run out of tries
                raise RuntimeError(
                    f"Upload of {change.name} persistently failing; could not get checksums to match."
                )

        return draft

    async def _upload_file(self, upload: _UploadSpec):
        if isinstance(upload.source, io.IOBase):
            wrapped_file = FileWrapper(upload.source.read())
        else:
            with upload.source.open("rb") as f:
                wrapped_file = FileWrapper(f.read())

        draft = await self.create_file(upload.dest, wrapped_file)

        wrapped_file.actually_close()
        return draft

    async def attach_datapackage(
        self,
        partitions_in_deposition: dict[str, Partitions],
    ) -> tuple[DataPackage, bool]:
        """Generate new datapackage describing draft deposition in current state."""
        new_datapackage = await self.generate_datapackage(partitions_in_deposition)

        datapackage_json = io.BytesIO(
            bytes(
                new_datapackage.model_dump_json(by_alias=True, indent=4),
                encoding="utf-8",
            )
        )
        await self.create_file("datapackage.json", datapackage_json)
        return new_datapackage


@dataclass
class DepositionBackend:
    """Wrap Published and Draft Deposition classes for a single depositor."""

    api_client: type[DepositorAPIClient]
    published_interface: type[PublishedDeposition]
    draft_interface: type[DraftDeposition]


DEPOSITION_BACKENDS: dict[str, DepositionBackend] = {}


def register_depositor(
    depositor_name: str,
    api_client: type[DepositorAPIClient],
    published_interface: type[PublishedDeposition],
    draft_interface: type[DraftDeposition],
):
    """Function to register an implementation of the depositor interface."""
    DEPOSITION_BACKENDS[depositor_name] = DepositionBackend(
        api_client=api_client,
        published_interface=published_interface,
        draft_interface=draft_interface,
    )

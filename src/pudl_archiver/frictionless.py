"""Core routines for frictionless data package construction."""

import zipfile
from collections.abc import Iterable
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

from pudl.metadata.classes import Contributor, DataSource, License
from pudl.metadata.constants import CONTRIBUTORS
from pydantic import BaseModel, Field, field_serializer

from pudl_archiver.utils import Url
from pudl_archiver.zenodo.entities import DepositionFile

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "txt": "text/csv",
}


class ZipLayout(BaseModel):
    """Define expected layout of a zipfile."""

    file_paths: set[Path]

    def validate_zip(self, file_path: Path) -> tuple[bool, list[str]]:
        """Validate that zipfile layout matches expectations."""
        # Avoid circular import
        from pudl_archiver.archivers.validate import _validate_file_type

        notes = []
        success = True
        with zipfile.ZipFile(file_path) as resource:
            files = {Path(name) for name in resource.namelist()}

            # Check that zipfile contains only expected files
            if files != self.file_paths:
                success = False
                if extra_files := list(map(str, files - self.file_paths)):
                    notes.append(
                        f"{file_path.name} contains unexpected files: {extra_files}"
                    )

                if missing_files := list(map(str, self.file_paths - files)):
                    notes.append(f"{file_path.name} is missing files: {missing_files}")

            # Check that all files in zipfile are valid based on their extension
            invalid_files = [
                f"The file, {str(filename)}, in {file_path.name} is invalid."
                for filename in files
                if not _validate_file_type(
                    filename, BytesIO(resource.read(str(filename)))
                )
            ]
            if len(invalid_files) > 0:
                notes += invalid_files
                success = False

        return success, notes


class ResourceInfo(BaseModel):
    """Class providing information about downloaded resource."""

    local_path: Path
    partitions: dict[str, Any]
    layout: ZipLayout | None = None


class Resource(BaseModel):
    """A generic data resource, as per Frictionless Data specs.

    See https://specs.frictionlessdata.io/data-resource.
    """

    profile: str = "data-resource"
    name: str
    path: Url
    remote_url: Url
    title: str
    parts: dict[str, Any]
    encoding: str = "utf-8"
    mediatype: str
    format_: str = Field(alias="format")
    bytes_: int = Field(alias="bytes")
    hash_: str = Field(alias="hash")

    @classmethod
    def from_file(cls, file: DepositionFile, parts: dict[str, str]) -> "Resource":
        """Create a resource from a single file with partitions.

        Args:
            file: Deposition file metadata returned by Zenodo api.
            parts: Working partitions of current resource.
        """
        filename = Path(file.filename)
        mt = MEDIA_TYPES[filename.suffix[1:]]

        return cls(
            name=file.filename,
            path=file.links.canonical,
            remote_url=file.links.canonical,
            title=filename.name,
            mediatype=mt,
            parts=parts,
            bytes=file.filesize,
            hash=file.checksum,
            format=filename.suffix,
        )


class DataPackage(BaseModel):
    """A generic Data Package, as per Frictionless Data specs.

    See https://specs.frictionlessdata.io/data-package.
    """

    name: str
    title: str
    description: str
    keywords: list[str]
    contributors: list[Contributor]
    sources: list[dict[str, str]]
    profile: str = "data-package"
    homepage: str = "https://catalyst.coop/pudl/"
    licenses: list[License]
    resources: list[Resource]
    created: str
    version: str | None = None

    @field_serializer("contributors", "licenses")
    def serialize_paths(self, contributors: list[Contributor | License], info):
        """Convert URLs to strings within certain types.

        Pydantic URL types no longer inherit from str, so when they are serialized they
        don't nicely become string representations of the URL like frictionless expects.

        Elsewhere this is handled using a custom type that handles serialization, but
        Contributor and License both come from PUDL so we need to manually convert their
        URLs.
        """
        serialized_contributors = []
        for contributor in contributors:
            # Pass kwargs from top-level model_dump call
            serialized = contributor.model_dump(**info.__dict__)
            serialized["path"] = str(serialized["path"])
            serialized_contributors.append(serialized)

        return serialized_contributors

    @classmethod
    def from_filelist(
        cls,
        name: str,
        files: Iterable[DepositionFile],
        resources: dict[str, ResourceInfo],
        version: str | None,
    ) -> "DataPackage":
        """Create a frictionless datapackage from a list of files and partitions.

        Args:
            name: Data source id.
            files: List file metadata returned by zenodo api.
            resources: A dictionary mapping file names to a ResourceInfo object containing
                       the local path to the resource, and its working partitions.
            version: Version string for current deposition version.
        """
        data_source = DataSource.from_id(name)

        return DataPackage(
            name=f"pudl-raw-{data_source.name}",
            title=f"PUDL Raw {data_source.title}",
            sources=[{"title": data_source.title, "path": str(data_source.path)}],
            licenses=[data_source.license_raw],
            resources=sorted(
                [
                    Resource.from_file(file, resources[file.filename].partitions)
                    for file in files
                ],
                key=lambda x: x.name,
            ),  # Sort by filename
            contributors=[CONTRIBUTORS["catalyst-cooperative"]],
            created=str(datetime.utcnow()),
            keywords=data_source.keywords,
            description=data_source.description,
            version=version,
        )

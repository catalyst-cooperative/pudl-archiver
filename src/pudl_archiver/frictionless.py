"""Core routines for frictionless data package construction."""

import datetime
import zipfile
from collections.abc import Iterable
from io import BytesIO
from pathlib import Path
from typing import Any

from pudl.metadata.classes import Contributor, DataSource, License
from pudl.metadata.constants import CONTRIBUTORS
from pudl.metadata.sources import SOURCES
from pydantic import BaseModel, Field, field_serializer

from pudl_archiver.metadata.sources import NON_PUDL_SOURCES
from pudl_archiver.utils import Url

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "txt": "text/csv",
    "parquet": "application/vnd.apache.parquet",
    "pdf": "application/pdf",
    "md": "text/markdown",
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
    title: str
    parts: dict[str, Any]
    encoding: str = "utf-8"
    mediatype: str
    format_: str = Field(alias="format")
    bytes_: int = Field(alias="bytes")
    hash_: str = Field(alias="hash")


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
            serialized = contributor.model_dump(
                # Pass kwargs from top-level model_dump call
                # Do not serialize null values, e.g. contributor ORCID which is optional
                **(info.__dict__ | {"exclude_none": True})
            )
            serialized["path"] = str(serialized["path"])
            serialized_contributors.append(serialized)

        return serialized_contributors

    @classmethod
    def new_datapackage(
        cls,
        name: str,
        resources: Iterable[Resource],
        version: str | None,
    ) -> "DataPackage":
        """Create a frictionless datapackage from a list of files and partitions.

        Args:
            name: Data source id.
            files: List file metadata returned by zenodo api.
            resources: A dictionary mapping file names to a ResourceInfo object
                containing the local path to the resource, and its working partitions.
            version: Version string for current deposition version.
        """
        if name in SOURCES:  # If data source in PUDL source metadata
            return cls.from_pudl_metadata(
                name=name, resources=resources, version=version
            )
        return cls.from_non_pudl_metadata(
            name=name, resources=resources, version=version
        )

    @classmethod
    def from_pudl_metadata(
        cls,
        name: str,
        resources: Iterable[Resource],
        version: str | None,
    ) -> "DataPackage":
        """Create a datapackage using PUDL metadata associated with ``name``."""
        data_source = DataSource.from_id(name, sources=SOURCES)

        return DataPackage(
            name=f"pudl-raw-{data_source.name}",
            title=f"PUDL Raw {data_source.title}",
            sources=[{"title": data_source.title, "path": str(data_source.path)}],
            licenses=[data_source.license_raw],
            resources=sorted(resources, key=lambda x: x.name),  # Sort by filename
            contributors=[CONTRIBUTORS["catalyst-cooperative"]],
            created=datetime.datetime.now(tz=datetime.UTC).isoformat(),
            keywords=data_source.keywords,
            description=data_source.description,
            version=version,
        )

    @classmethod
    def from_non_pudl_metadata(
        cls,
        name: str,
        resources: Iterable[Resource],
        version: str | None,
    ):
        """Create a datapackage for sources that won't end up in PUDL."""
        data_source = DataSource.from_id(name, sources=NON_PUDL_SOURCES)

        return DataPackage(
            name=data_source.name,
            title=data_source.title,
            sources=[{"title": data_source.title, "path": str(data_source.path)}],
            licenses=[data_source.license_raw],
            resources=sorted(resources, key=lambda x: x.name),  # Sort by filename
            # Make it easier to add contributors directly into source dictionary
            contributors=data_source.contributors,
            created=datetime.datetime.now(tz=datetime.UTC).isoformat(),
            keywords=data_source.keywords,
            description=data_source.description,
            version=version,
        )

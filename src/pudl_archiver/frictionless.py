"""Core routines for frictionless data package construction."""
from datetime import datetime
from pathlib import Path

from pydantic import AnyHttpUrl, BaseModel, Field

from pudl.metadata.classes import Contributor, DataSource, Datetime, License
from pudl.metadata.constants import CONTRIBUTORS
from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.zenodo.entities import DepositionFile

MEDIA_TYPES: dict[str, str] = {
    "zip": "application/zip",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
}


class Resource(BaseModel):
    """A generic data resource, as per Frictionless Data specs.

    See https://specs.frictionlessdata.io/data-resource.
    """

    profile: str = "data-resource"
    name: str
    path: AnyHttpUrl
    remote_url: AnyHttpUrl
    title: str
    parts: dict
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
            path=file.links.download,
            remote_url=file.links.download,
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
    created: Datetime

    @classmethod
    def from_filelist(
        cls, name: str, files: list[DepositionFile], resources: dict[str, ResourceInfo]
    ) -> "DataPackage":
        """Create a frictionless datapackage from a list of files and partitions.

        Args:
            name: Data source id.
            files: List file metadata returned by zenodo api.
            resources: A dictionary mapping file names to a ResourceInfo object containing
                       the local path to the resource, and its working partitions.
        """
        data_source = DataSource.from_id(name)

        return DataPackage(
            name=f"pudl-raw-{data_source.name}",
            title=f"PUDL Raw {data_source.title}",
            sources=[{"title": data_source.title, "path": data_source.path}],
            licenses=[data_source.license_raw],
            resources=[
                Resource.from_file(file, resources[file.filename].partitions)
                for file in files
            ],
            contributors=[CONTRIBUTORS["catalyst-cooperative"]],
            created=datetime.utcnow(),
            keywords=data_source.keywords,
            description=data_source.description,
        )

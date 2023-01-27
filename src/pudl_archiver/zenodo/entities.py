"""Models defining zenodo api entities.

See https://developers.zenodo.org/#entities for more info.
"""
import datetime
import re
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConstrainedStr, Field, validator

from pudl.metadata.classes import Contributor, DataSource


class Doi(ConstrainedStr):
    """The DOI format for production Zenodo."""

    regex = re.compile(r"10\.5281/zenodo\.\d{6,7}")


class SandboxDoi(ConstrainedStr):
    """The DOI format for sandbox Zenodo."""

    regex = re.compile(r"10\.5072/zenodo\.\d{6,7}")


PUDL_DESCRIPTION = """
<p>This archive contains raw input data for the Public Utility Data Liberation (PUDL)
software developed by <a href="https://catalyst.coop">Catalyst Cooperative</a>. It is
organized into <a href="https://specs.frictionlessdata.io/data-package/">Frictionless
Data Packages</a>. For additional information about this data and PUDL, see the
following resources:
<ul>
  <li><a href="https://github.com/catalyst-cooperative/pudl">The PUDL Repository on GitHub</a></li>
  <li><a href="https://catalystcoop-pudl.readthedocs.io">PUDL Documentation</a></li>
  <li><a href="https://zenodo.org/communities/catalyst-cooperative/">Other Catalyst Cooperative data archives</a></li>
</ul>
</p>
"""


class DepositionCreator(BaseModel):
    """Pydantic model representing zenodo deposition creators.

    See https://developers.zenodo.org/#representation.
    """

    name: str
    affiliation: str | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "DepositionCreator":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(name=contributor.title, affiliation=contributor.organization)


class DepositionMetadata(BaseModel):
    """Pydantic model representing zenodo deposition metadata.

    See https://developers.zenodo.org/#representation.
    """

    upload_type: str = "dataset"
    publication_date: datetime.date = None
    language: str = "eng"
    title: str
    creators: list[DepositionCreator]
    description: str
    access_right: str = "open"
    license_: str = Field(alias="license")
    doi: Doi | SandboxDoi | None = None
    prereserve_doi: dict | bool = False
    keywords: list[str] | None = None
    version: str | None = None

    @validator("doi", pre=True)
    def check_empty_string(cls, doi: str):  # noqa: N805
        """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
        if doi == "":
            return None

    @classmethod
    def from_data_source(cls, data_source_id: str) -> "DepositionMetadata":
        """Construct deposition metadata object from PUDL DataSource model."""
        data_source = DataSource.from_id(data_source_id)
        creators = [
            DepositionCreator.from_contributor(contributor)
            for contributor in data_source.contributors
        ]

        if not creators:
            creators = [
                DepositionCreator.from_contributor(
                    Contributor.from_id("catalyst-cooperative")
                )
            ]

        return cls(
            title=f"PUDL Raw {data_source.title}",
            description=(
                f"<p>{data_source.description} Archived from \n"
                f'<a href="{data_source.path}">{data_source.path}</a></p>'
                f"{PUDL_DESCRIPTION}"
            ),
            creators=creators,
            license=data_source.license_raw.name,
            keywords=data_source.keywords,
            version="1.0.0",
        )


class FileLinks(BaseModel):
    """Pydantic model representing zenodo deposition file Links."""

    self: AnyHttpUrl | None = None
    version: AnyHttpUrl | None = None
    uploads: AnyHttpUrl | None = None
    download: AnyHttpUrl | None = None


class BucketFile(BaseModel):
    """Pydantic model representing zenodo file metadata returned by bucket api.

    See https://developers.zenodo.org/#quickstart-upload.
    """

    key: str
    mimetype: str
    checksum: str
    version_id: str
    size: int
    created: datetime.datetime
    updated: datetime.datetime
    links: FileLinks
    is_head: bool
    delete_marker: bool


class DepositionFile(BaseModel):
    """Pydantic model representing zenodo deposition files.

    See https://developers.zenodo.org/#representation22.
    """

    checksum: str
    filename: str
    id_: str = Field(alias="id")
    filesize: int
    links: FileLinks


class DepositionLinks(BaseModel):
    """Pydantic model representing zenodo deposition Links."""

    bucket: AnyHttpUrl | None = None
    discard: AnyHttpUrl | None = None
    edit: AnyHttpUrl | None = None
    files: AnyHttpUrl | None = None
    html: AnyHttpUrl | None = None
    latest_draft: AnyHttpUrl | None = None
    latest_draft_html: AnyHttpUrl | None = None
    publish: AnyHttpUrl | None = None
    self: AnyHttpUrl | None = None


class Deposition(BaseModel):
    """Pydantic model representing a zenodo deposition.

    See https://developers.zenodo.org/#depositions.
    """

    conceptdoi: Doi | SandboxDoi | None = None
    conceptrecid: str
    created: datetime.datetime
    files: list[DepositionFile] = []
    id_: int = Field(alias="id")
    metadata: DepositionMetadata
    modified: datetime.datetime
    links: DepositionLinks
    owner: int
    record_id: int
    record_url: AnyHttpUrl | None = None
    state: Literal["inprogress", "done", "error", "submitted", "unsubmitted"]
    submitted: bool
    title: str

"""Models defining zenodo api entities.

See https://developers.zenodo.org/#entities for more info.
"""

import datetime
import logging
import re
from typing import Annotated, Literal

from pudl.metadata.classes import Contributor, DataSource
from pydantic import BaseModel, Field, StringConstraints, field_validator

from pudl_archiver.utils import Url

logger = logging.getLogger(f"catalystcoop.{__name__}")

Doi = Annotated[str, StringConstraints(pattern=r"10\.5281/zenodo\.\d+")]
SandboxDoi = Annotated[str, StringConstraints(pattern=r"10\.5072/zenodo\.\d+")]

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


class DatasetSettings(BaseModel):
    """Simple model to validate doi's in settings."""

    production_doi: Doi | None = None
    sandbox_doi: SandboxDoi | None = None


class ZenodoClientError(Exception):
    """Captures the JSON error information from Zenodo."""

    def __init__(self, status, message, errors=None):
        """Constructor.

        Args:
            status: status message of response
            message: message of response
            errors: if any, list of errors returned by response
        """
        self.status = status
        self.message = message
        self.errors = errors

    def __str__(self):
        """Cast to string."""
        return repr(self)

    def __repr__(self):
        """But the kwargs are useful for recreating this object."""
        return f"ZenodoClientError(status={self.status}, message={self.message}, errors={self.errors})"


class DepositionCreator(BaseModel):
    """Pydantic model representing zenodo deposition creators.

    See https://developers.zenodo.org/#representation.
    """

    name: str
    affiliation: str | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "DepositionCreator":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(
            name=contributor.title,
            affiliation=contributor.organization,
        )


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

    @field_validator("doi", mode="before")
    @classmethod
    def check_empty_string(cls, doi: str):  # noqa: N805
        """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
        if doi == "":
            return

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

        # If data source was manually archived by us, specify that the
        # data_source.path is a documentation link, rather than where we archived
        # the data from.
        if data_source_id in ["gridpathratoolkit", "vcerare"]:
            return cls(
                title=f"{data_source.title}",
                description=(
                    f"<p>{data_source.description}</p> <p>Archived by Catalyst \n"
                    "Cooperative from data provided directly from the dataset's \n"
                    "creator. For more information, see \n"
                    f'<a href="{data_source.path}">{data_source.path}</a></p>'
                    f"{PUDL_DESCRIPTION}"
                ),
                creators=creators,
                license=data_source.license_raw.name,
                keywords=data_source.keywords,
                version="1.0.0",
            )

        # Otherwise, specify that data was archived from the data_source.path
        # and can be found there.
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

    self: Url | None = None
    version: Url | None = None
    uploads: Url | None = None
    download: Url | None = None

    @property
    def canonical(self):
        """The most stable URL that points at this file.

        Can be:
        https://zenodo.org/records/<record_id>/files/<filename>
        https://www.zenodo.org/records/<record_id>/files/<filename>
        https://sandbox.zenodo.org/records/<record_id>/files/<filename>

        We extract the record ID and filename from the file's download link.
        """
        match = re.match(
            r"(?P<base_url>https?://.*zenodo.org).*"
            r"(?P<record_id>/records/\d+).*"
            r"(?P<filename>/files/[^/]+)",
            str(self.download),
        )
        if match is None:
            raise ValueError(f"Got bad Zenodo download URL: {self.download}")
        return "".join(match.groups())


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

    bucket: Url | None = None
    discard: Url | None = None
    edit: Url | None = None
    files: Url | None = None
    html: Url | None = None
    latest_draft: Url | None = None
    latest_draft_html: Url | None = None
    publish: Url | None = None
    self: Url | None = None


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
    record_url: Url | None = None
    state: Literal["inprogress", "done", "error", "submitted", "unsubmitted"]
    submitted: bool
    title: str

    @property
    def files_map(self) -> dict[str, DepositionFile]:
        """Files associated with their filenames."""
        return {f.filename: f for f in self.files}


class Record(BaseModel):
    """The /records/ endpoints return a slightly different data structure."""

    id_: int = Field(alias="id")
    links: DepositionLinks

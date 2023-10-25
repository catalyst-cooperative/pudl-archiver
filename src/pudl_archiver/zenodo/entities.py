"""Models defining zenodo api entities.

See https://developers.zenodo.org/#entities for more info.
"""
import datetime
import logging
import re
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConstrainedStr, Field, validator

from pudl.metadata.classes import Contributor, DataSource

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Doi(ConstrainedStr):
    """The DOI format for production and sandbox Zenodo."""

    regex = re.compile(r"10\.5281/zenodo\.\d{6,7}")


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


class Person(BaseModel):
    """Pydantic model representing individual Zenodo deposition creators."""

    type_: Literal["personal", "organizational"] = Field(alias="type")
    given_name: str
    family_name: str
    identifiers: dict[str, str] | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Person":
        """Construct deposition metadata object from PUDL Contributor model."""
        # Split name into first and last. Spit warning if this splits into three.
        names = contributor.title.split(" ")
        if len(names) > 2:
            logger.warning(
                f"Contributor {contributor.title} has name length > 2. Only taking first and last name."
            )
        given_name = names[0]
        family_name = names[-1]

        # Right now we only handle ORCIDs, we could include other supported ID schemes.
        if contributor.orcid:
            identifiers = {"scheme": "orcid", "identifier": contributor.orcid}
        else:
            identifiers = {}

        return cls(
            type="personal",
            given_name=given_name,
            family_name=family_name,
            identifiers=identifiers,
        )


class Organization(BaseModel):
    """Pydantic model representing organizational Zenodo deposition creators."""

    type_: Literal["personal", "organizational"] = Field(alias="type")
    name: str
    identifiers: dict[str, str] | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Organization":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(type="organizational", name=contributor.title)


class Affiliation(BaseModel):
    """Pydantic model representing organization affiliations of deposition creators."""

    id_: str | None = Field(alias="id")
    name: str | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Affiliation":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(name=contributor.organization)


class Role(BaseModel):
    """Pydantic model representing organization roles of deposition creators."""

    id_: str | None = Field(alias="id")

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Role":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(id=contributor.role)


class DepositionCreator(BaseModel):
    """Pydantic model representing zenodo deposition creators.

    See https://inveniordm.docs.cern.ch/reference/metadata/#creators-1-n.
    """

    person_or_org: Person | Organization
    affiliation: Affiliation | str | None = (
        None  # String is for pre-migration datapackages.
    )
    role: Role | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "DepositionCreator":
        """Construct deposition metadata object from PUDL Contributor model."""
        if contributor.title != contributor.organization:
            person_or_org = Person.from_contributor(contributor)
        else:
            person_or_org = Organization.from_contributor(contributor)  # Debug
        return cls(
            person_or_org=person_or_org,
            affiliation=Affiliation.from_contributor(contributor),
            role=Role.from_contributor(contributor),
        )


class License(BaseModel):
    """Pydantic model representing dataset licenses for Zenodo deposition."""

    id_: str | None = Field(alias="id")
    title: str | None = None
    description: str | None = None
    link: AnyHttpUrl | None = None

    @classmethod
    def from_data_source(cls, data_source: str) -> "License":
        """Construct deposition metadata object from PUDL Contributor model."""
        license_raw = data_source.license_raw
        # Can only provide ID or title to Zenodo, not both.
        if license_raw.name == "CC-BY-4.0":
            license_id = license_raw.name.lower()
            title = None
        else:
            title = license_raw.name
            license_id = None
        link = license_raw.path
        return cls(id=license_id, title=title, link=link)


class DepositionMetadata(BaseModel):
    """Pydantic model representing zenodo deposition metadata.

    See https://developers.zenodo.org/#representation.
    """

    publication_date: datetime.date = None
    language: str = "eng"
    title: str
    creators: list[DepositionCreator]
    communities: list[dict] | None = None
    description: str
    access_right: str = "open"
    license_: License | None = Field(alias="license")
    doi: Doi | None = None
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
            license=License.from_data_source(data_source),
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

    self: AnyHttpUrl | None = None
    self_html: AnyHttpUrl | None = None
    parent_doi: AnyHttpUrl | None = None
    self_iiif_manifest: AnyHttpUrl | None = None
    self_iiif_sequency: AnyHttpUrl | None = None
    files: AnyHttpUrl | None = None
    media_files: AnyHttpUrl | None = None
    archive: AnyHttpUrl | None = None
    archive_media: AnyHttpUrl | None = None
    record: AnyHttpUrl | None = None
    record_html: AnyHttpUrl | None = None
    publish: AnyHttpUrl | None = None
    review: AnyHttpUrl | None = None
    versions: AnyHttpUrl | None = None
    access_links: AnyHttpUrl | None = None
    access_users: AnyHttpUrl | None = None
    access_request: AnyHttpUrl | None = None
    access: AnyHttpUrl | None = None
    reserve_doi: AnyHttpUrl | None = None
    communities: AnyHttpUrl | None = None
    communities_suggestions: AnyHttpUrl | None = None
    requests: AnyHttpUrl | None = None


class Deposition(BaseModel):
    """Pydantic model representing a zenodo deposition.

    See https://developers.zenodo.org/#depositions.
    """

    conceptdoi: Doi
    conceptrecid: str
    created: datetime.datetime
    files: list[DepositionFile] = []
    id_: int = Field(alias="id")
    metadata: DepositionMetadata
    modified: datetime.datetime
    links: DepositionLinks
    owners: list[dict] = []
    recid: int
    record_url: AnyHttpUrl | None = None
    resource_type: dict[str, str] = {"id": "dataset"}
    state: Literal["inprogress", "done", "error", "submitted", "unsubmitted"]
    submitted: bool
    title: str

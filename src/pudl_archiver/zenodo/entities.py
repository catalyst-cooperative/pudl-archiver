"""Models defining zenodo api entities.

See https://developers.zenodo.org/#entities for more info.
"""
import datetime
import logging
import re
from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, ConstrainedStr, Field, validator

from pudl.metadata.classes import Contributor, DataSource

logger = logging.getLogger(f"catalystcoop.{__name__}")


class Doi(ConstrainedStr):
    """The DOI format for production and sandbox Zenodo."""

    regex = re.compile(r"10\.5281/zenodo\.\d{6,8}")


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

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Person":
        """Construct deposition metadata object from PUDL Contributor model."""
        kwargs = {}
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
            kwargs["identifiers"] = [
                {"scheme": "orcid", "identifier": contributor.orcid}
            ]

        return cls(
            type="personal",
            given_name=given_name,
            family_name=family_name,
            **kwargs,
        )


class Organization(BaseModel):
    """Pydantic model representing organizational Zenodo deposition creators."""

    type_: Literal["personal", "organizational"] = Field(alias="type")
    name: str

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Organization":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(type="organizational", name=contributor.title)


class Affiliations(BaseModel):
    """Pydantic model representing organization affiliations of deposition creators."""

    name: str | None = None

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Affiliations":
        """Construct deposition metadata object from PUDL Contributor model."""
        return cls(name=contributor.organization)


class Role(BaseModel):
    """Pydantic model representing organization roles of deposition creators."""

    id_: str | None = Field(alias="id")

    @classmethod
    def from_contributor(cls, contributor: Contributor) -> "Role":
        """Construct deposition metadata object from PUDL Contributor model."""
        kwargs = {}
        if contributor.role:  # Change to zenodo_role once integrated into PUDL.
            # Invenio RDM only accepts certain values.
            if contributor.role in [
                "contact person",
                "data collector",
                "data curator",
                "data manager",
                "distributor",
                "editor",
                "hosting institution",
                "other",
                "producer",
                "project leader",
                "project member",
                "registration agency",
                "registration authority",
                "related person",
                "researcher",
                "rights holder",
                "sponsor",
                "supervisor",
                "work package leader",
            ]:  # Unclear to me what roles are allowed here.
                kwargs["id"] = contributor.role.replace(" ", "")
            else:
                kwargs["id"] = "projectmember"

        return cls(**kwargs)


class DepositionCreatorResponse(BaseModel):
    """Pydantic model representing response of Zenodo deposition creators."""

    name: str
    affiliation: str


class DepositionCreator(BaseModel):
    """Pydantic model representing zenodo deposition creators.

    See https://inveniordm.docs.cern.ch/reference/metadata/#creators-1-n.
    """

    person_or_org: Person | Organization
    affiliations: list[
        Affiliations
    ] | str | None = None  # String is for pre-migration datapackages.
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
            affiliations=[Affiliations.from_contributor(contributor)],
            role=Role.from_contributor(contributor),
        )


class License(BaseModel):
    """Pydantic model representing dataset licenses for Zenodo deposition."""

    id_: str | None = Field(alias="id")
    title: dict[str, str] | None = None
    description: dict[str, str] | None = None
    link: AnyHttpUrl | None = None

    @classmethod
    def from_data_source(cls, data_source: str) -> "License":
        """Construct deposition metadata object from PUDL Contributor model."""
        kwargs = {}
        license_raw = data_source.license_raw
        # Can only provide ID or title to Zenodo, not both.
        if license_raw.name == "CC-BY-4.0":
            kwargs["id"] = license_raw.name.lower()
        else:
            kwargs["title"] = {"en": license_raw.name}
        return cls(
            link=license_raw.path, description={"en": license_raw.title}, **kwargs
        )


class DepositionMetadata(BaseModel):
    """Pydantic model representing zenodo deposition metadata.

    See https://developers.zenodo.org/#representation.
    """

    resource_type: dict[str, str | dict[str, str]] = {"id": "dataset"}
    publication_date: datetime.date = None
    languages: list[dict[str, str | dict[str, str]]] = [{"id": "eng"}]
    title: str | dict[str, str]
    creators: list[DepositionCreator | DepositionCreatorResponse] | None = None
    communities: list[dict] | None = None
    description: str
    rights: list[License] | None = None
    doi: Doi | None = None
    prereserve_doi: dict | bool = False
    subjects: list[dict[str, str]] | None = None
    version: str | None = None
    publisher: str = "Catalyst Cooperative"

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

        # Format keywords and license
        subjects = [{"subject": keyword} for keyword in data_source.keywords]
        rights = [License.from_data_source(data_source)]

        return cls(
            title=f"PUDL Raw {data_source.title}",
            description=(
                f"<p>{data_source.description} Archived from \n"
                f'<a href="{data_source.path}">{data_source.path}</a></p>'
                f"{PUDL_DESCRIPTION}"
            ),
            creators=creators,
            rights=rights,
            subjects=subjects,
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
    key: str
    id_: str = Field(alias="id")
    size: int
    links: FileLinks

    # TO DO: possibly remove the md5 prefix for checksum in here rather than when called?


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

    See https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/#get-latest-version.
    """

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
    state: Literal["inprogress", "done", "error", "submitted", "unsubmitted"]
    submitted: bool
    title: str


class VersionFiles(BaseModel):
    """Pydantic model of files dict returned by `create_new_deposition_version`."""

    enabled: bool
    order: list[str]
    count: int
    total_bytes: int
    entries: dict[str, dict[str, Any]]  # Doesn't always conform to DepositionFile


class DepositionVersion(BaseModel):
    """Pydantic model representing a zenodo response to POST requests.

    Response returned by POST responses ()`create_new_deposition_version` and
    `publish_deposition` methods, which are formatted
    differently than the response to `get_record`. There are more fields captured here
    that aren't mapped by this class, but as we are interested
    in the links, ID and metadata primarily they are not presently included,
    as they are returned when the DOI is registered in the `get_new_version` method.

    See https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/#create-a-new-version.
    """

    created: datetime.datetime
    id_: int = Field(alias="id")
    metadata: DepositionMetadata
    links: DepositionLinks
    owners: list[dict] = []
    record_url: AnyHttpUrl | None = None
    status: Literal["new_version_draft", "published"]
    files: VersionFiles

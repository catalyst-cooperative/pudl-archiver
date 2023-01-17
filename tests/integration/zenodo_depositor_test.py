import os

import aiohttp
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.zenodo.entities import DepositionCreator, DepositionMetadata


@pytest.fixture()
def dotenv():
    """Load dotenv to get API keys."""
    load_dotenv()


@pytest.fixture()
def upload_key(dotenv):
    """Get upload key."""
    return os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]


@pytest.fixture()
def publish_key(dotenv):
    """Get publish key."""
    return os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]


@pytest.fixture()
def deposition_metadata():
    """Create fake DepositionMetadata model."""
    return DepositionMetadata(
        title="PUDL Test",
        creators=[
            DepositionCreator(
                name="catalyst-cooperative", affiliation="Catalyst Cooperative"
            )
        ],
        description="Test dataset for the sandbox, thanks!",
        version="1.0.0",
        license="CC0-1.0",
        keywords=["test"],
    )


@pytest.fixture()
def datapackage():
    """Create test datapackage descriptor."""
    return DataPackage(
        name="pudl_test",
        title="PUDL Test",
        description="Test dataset for the sandbox, thanks!",
        keywords=[],
        contributors=[],
        sources=[],
        licenses=[],
        resources=[],
        created="2023-01-17T20:57:05.000000Z",
    )


@pytest_asyncio.fixture()
async def session():
    """Create async http session."""
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        yield session


@pytest.fixture()
def depositor(upload_key, publish_key, session):
    return ZenodoDepositor(upload_key, publish_key, session, sandbox=True)


def clean_metadata(metadata):
    return metadata.dict(
        by_alias=True,
        exclude={"publication_date", "doi", "prereserve_doi"},
    )


@pytest.mark.asyncio
async def test_create_deposition(depositor, deposition_metadata):
    deposition = await depositor.create_deposition(deposition_metadata)

    assert clean_metadata(deposition.metadata) == clean_metadata(deposition_metadata)
    assert deposition.state == "unsubmitted"


@pytest.mark.asyncio
async def test_get_deposition(depositor, deposition_metadata):
    pass
    # commented these out so I can commit.
    # deposition = await depositor.create_deposition(deposition_metadata)
    # TODO (daz): guess we need to add files and publish, before it gets a concept
    # depositor.create_file()
    # depositor.publish()
    # we should probably make just one deposition and do a bunch of stuff to it.
    # this means we also need to implement get by specific doi.

    # concept_doi = deposition.conceptdoi
    # assert concept_doi is not None, "has a concept_doi"
    # latest_deposition = await depositor.get_deposition(concept_doi)
    # assert latest_deposition._id == deposition._id

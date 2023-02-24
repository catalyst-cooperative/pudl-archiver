import asyncio
import os
from io import BytesIO

import aiohttp
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.depositors.zenodo import ZenodoClientException
from pudl_archiver.utils import retry_async
from pudl_archiver.zenodo.entities import DepositionCreator, DepositionMetadata


@pytest_asyncio.fixture()
async def session():
    """Create async http session."""
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture()
def depositor(session):
    load_dotenv()
    upload_key = os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]
    publish_key = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
    return ZenodoDepositor(upload_key, publish_key, session, sandbox=True)


def clean_metadata(metadata):
    return metadata.dict(
        by_alias=True,
        exclude={"publication_date", "doi", "prereserve_doi"},
    )


@pytest_asyncio.fixture()
async def empty_deposition(depositor):
    deposition_metadata = DepositionMetadata(
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

    deposition = await depositor.create_deposition(deposition_metadata)

    assert clean_metadata(deposition.metadata) == clean_metadata(deposition_metadata)
    assert deposition.state == "unsubmitted"
    return deposition


@pytest_asyncio.fixture()
async def initial_deposition(depositor, empty_deposition):
    initial_files = {
        "to_update": b"I am outdated",
        "to_delete": b"Delete me!",
    }
    await depositor.create_file(
        empty_deposition,
        "to_update",
        BytesIO(initial_files["to_update"]),
        force_api="files",
    )
    await depositor.create_file(
        empty_deposition, "to_delete", BytesIO(initial_files["to_delete"])
    )

    # publish initial deposition
    return await depositor.publish_deposition(empty_deposition)


async def get_latest(depositor, conceptdoi, published_only=False):
    return await retry_async(
        depositor.get_deposition,
        args=[conceptdoi],
        kwargs={"published_only": published_only},
        retry_on=(
            ZenodoClientException,
            aiohttp.ClientError,
            asyncio.TimeoutError,
            IndexError,
        ),
    )


@pytest.mark.asyncio()
async def test_publish_empty(depositor, empty_deposition, mocker):
    # try publishing empty
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    with pytest.raises(ZenodoClientException) as excinfo:
        await depositor.publish_deposition(empty_deposition)
    error_json = excinfo.value.kwargs["json"]
    assert "validation error" in error_json["message"].lower()
    assert (
        "minimum one file must be provided"
        in error_json["errors"][0]["message"].lower()
    )


@pytest.mark.asyncio()
async def test_delete_deposition(depositor, initial_deposition):
    """Make a new draft, delete it, and see that the conceptdoi still points
    at the original."""
    draft = await depositor.get_new_version(initial_deposition)

    latest = await get_latest(
        depositor, initial_deposition.conceptdoi, published_only=False
    )
    assert latest.id_ == draft.id_
    assert not latest.submitted

    await depositor.delete_deposition(draft)

    latest = await get_latest(
        depositor, initial_deposition.conceptdoi, published_only=True
    )
    assert latest.id_ == initial_deposition.id_


@pytest.mark.asyncio()
async def test_get_new_version_clobbers(depositor, initial_deposition):
    """Make a new draft, delete it, and see that the conceptdoi still points
    at the original."""

    bad_draft = await depositor.get_new_version(initial_deposition)
    non_clobbering = await depositor.get_new_version(initial_deposition, clobber=False)
    assert bad_draft.id_ == non_clobbering.id_

    latest = await get_latest(
        depositor, initial_deposition.conceptdoi, published_only=False
    )
    assert latest.id_ == bad_draft.id_

    clobbering = await depositor.get_new_version(initial_deposition, clobber=True)
    assert bad_draft.id_ == clobbering.id_

    latest = await get_latest(
        depositor, initial_deposition.conceptdoi, published_only=False
    )
    assert latest.id_ == clobbering.id_

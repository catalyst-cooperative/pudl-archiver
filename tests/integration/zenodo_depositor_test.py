import asyncio
import os
from io import BytesIO

import aiohttp
import pytest
import pytest_asyncio
from dotenv import load_dotenv
from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.depositors.zenodo import ZenodoClientError
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
        license="cc-zero",
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
            ZenodoClientError,
            aiohttp.ClientError,
            asyncio.TimeoutError,
            IndexError,
        ),
    )


@pytest.mark.asyncio()
async def test_publish_empty(depositor, empty_deposition, mocker):
    # try publishing empty
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    with pytest.raises(ZenodoClientError) as excinfo:
        await depositor.publish_deposition(empty_deposition)
    assert "validation error" in excinfo.value.message.lower()
    assert "missing uploaded files" in excinfo.value.errors[0]["messages"][0].lower()


@pytest.mark.asyncio()
async def test_delete_deposition(depositor, initial_deposition, mocker):
    """Make a new draft, delete it, and see that the conceptdoi still points
    at the original."""
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    draft = await depositor.get_new_version(initial_deposition, data_source_id="test")

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
async def test_get_new_version_clobbers(depositor, initial_deposition, mocker):
    """Make a new draft, test that not clobbering it returns an error, then
    delete it, and see that the conceptdoi still points
    at the original."""
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    clobberee = await depositor.get_new_version(initial_deposition, data_source_id = "test")
    with pytest.raises(ZenodoClientError) as excinfo:
        await depositor.get_new_version(
            initial_deposition, data_source_id="test", clobber=False
        )
        assert (
            "remove all files first" in excinfo.value.errors[0]["messages"][0].lower()
        )

    clobberer = await depositor.get_new_version(initial_deposition, data_source_id = "test", clobber=True)
    # assuming that ID is a monotonically increasing number, the clobberer should be bigger than the clobberee
    assert clobberee.id_ < clobberer.id_
    assert initial_deposition.conceptdoi == clobberer.conceptdoi

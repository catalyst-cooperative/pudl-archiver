import os
from io import BytesIO

import aiohttp
import pytest
import pytest_asyncio
import requests
from dotenv import load_dotenv

from pudl_archiver.depositors import ZenodoDepositor
from pudl_archiver.depositors.zenodo import ZenodoClientException
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
    async with aiohttp.ClientSession() as session:
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
async def test_full_zenodo_flow(depositor, deposition_metadata, upload_key):
    # create deposition
    deposition = await depositor.create_deposition(deposition_metadata)

    assert clean_metadata(deposition.metadata) == clean_metadata(deposition_metadata)
    assert deposition.state == "unsubmitted"

    # try publishing empty
    with pytest.raises(ZenodoClientException) as excinfo:
        await depositor.publish_deposition(deposition)
    error_json = excinfo.value.kwargs["json"]
    assert "validation error" in error_json["message"].lower()
    assert (
        "minimum one file must be provided"
        in error_json["errors"][0]["message"].lower()
    )

    conceptrecid = deposition.conceptrecid

    # add files to initial deposition
    initial_files = {
        "to_update": b"I am outdated",
        "to_delete": b"Delete me!",
    }

    await depositor.create_file(
        deposition, "to_update", BytesIO(initial_files["to_update"]), force_api="files"
    )
    await depositor.create_file(
        deposition, "to_delete", BytesIO(initial_files["to_delete"])
    )

    # publish initial deposition
    published_deposition = await depositor.publish_deposition(deposition)
    assert published_deposition.conceptdoi.rsplit(".", 1)[1] == conceptrecid
    assert published_deposition.id_ == deposition.id_
    assert published_deposition.state == "done"

    conceptdoi = published_deposition.conceptdoi

    # verify that the first version has the files we expect
    v1_files = published_deposition.files
    assert len(v1_files) == 2

    for deposition_file in v1_files:
        download_link = deposition_file.links.download
        expected_contents = initial_files[deposition_file.filename]

        remote_contents = requests.get(
            download_link, headers={"Authorization": f"bearer {upload_key}"}
        )
        assert expected_contents == remote_contents.text.encode("utf-8")

    # check that the latest deposition in the conceptdoi points at the one we just published
    latest_deposition = await depositor.get_deposition(conceptdoi)
    assert latest_deposition.id_ == published_deposition.id_

    new_deposition = await depositor.get_new_version(published_deposition)
    doubly_new_deposition = await depositor.get_new_version(new_deposition)

    # if we call get_new_version on an unsubmitted deposition, we should just get
    # that one back.
    assert new_deposition.id_ == doubly_new_deposition.id_

    updated_files = {
        "to_update": b"I am up to date",
    }
    await depositor.delete_file(new_deposition, "to_delete")
    await depositor.update_file(
        new_deposition, "to_update", BytesIO(updated_files["to_update"])
    )

    latest_deposition = await depositor.get_deposition(conceptdoi)
    assert latest_deposition.state == "unsubmitted"
    assert latest_deposition.id_ == new_deposition.id_

    # verify that the second version has the files we expect
    v2_files = latest_deposition.files
    assert len(v2_files) == 1

    for deposition_file in v2_files:
        download_link = deposition_file.links.download
        expected_contents = updated_files[deposition_file.filename]

        remote_contents = requests.get(
            download_link, headers={"Authorization": f"bearer {upload_key}"}
        )
        assert expected_contents == remote_contents.text.encode("utf-8")
"""Test zenodo client api."""
import asyncio
import os
import tempfile
import time
from pathlib import Path

import aiohttp
import pytest
import pytest_asyncio
import requests
from dotenv import load_dotenv

from pudl.metadata.classes import DataSource
from pudl.metadata.constants import LICENSES
from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.archivers.validate import Unchanged
from pudl_archiver.depositors.zenodo import ZenodoClientException
from pudl_archiver.orchestrator import DepositionOrchestrator
from pudl_archiver.utils import retry_async
from pudl_archiver.zenodo.entities import (
    Deposition,
    DepositionCreator,
    DepositionMetadata,
    Organization,
)


@pytest.fixture()
def dotenv():
    """Load dotenv to get API keys."""
    load_dotenv()


@pytest.fixture()
def deposition_metadata():
    """Create fake DepositionMetadata model."""
    return DepositionMetadata(
        title="PUDL Test",
        creators=[
            DepositionCreator(
                person_or_org=Organization(
                    name="catalyst-cooperative", type="organizational"
                ),
                affiliations=[{"name": "catalyst-cooperative"}],
                role={"id": "projectmember"},
            )
        ],
        description="Test dataset for the sandbox, thanks!",
        version="1.0.0",
        license={"id": "CC0-1.0"},
        subjects=[{"subject": "test"}],
    )


@pytest.fixture()
def upload_key(dotenv):
    """Get upload key."""
    return os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]


@pytest.fixture()
def publish_key(dotenv):
    """Get publish key."""
    return os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]


@pytest_asyncio.fixture()
async def session():
    """Create async http session."""
    async with aiohttp.ClientSession(raise_for_status=False) as session:
        yield session


@pytest_asyncio.fixture()
def test_files():
    """Create files for testing in temporary directory."""
    file_data = {
        "unchanged_file.txt": {
            "original": b"This file should not change during deposition update.",
            "updated": b"This file should not change during deposition update.",
        },
        "updated_file.txt": {
            "original": b"This file should updated during deposition update.",
            "updated": b"These are the updates.",
        },
        "deleted_file.txt": {
            "original": b"This file should deleted during deposition update.",
        },
        "created_file.txt": {
            "updated": b"This file should created during deposition update.",
        },
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        file_paths = {
            "original": tmp_dir_path / "original",
            "updated": tmp_dir_path / "updated",
        }

        # Create directories
        file_paths["original"].mkdir()
        file_paths["updated"].mkdir()

        # Loop through files and create them on local disk
        files = {"original": [], "updated": []}
        for filename, data in file_data.items():
            for dep_type, contents in data.items():
                path = file_paths[dep_type] / filename
                # Add to files
                files[dep_type].append({"path": path, "contents": contents})

                with open(path, "wb") as f:
                    f.write(contents)

        yield files


@pytest.fixture()
def datasource():
    """Create fake datasource for testing."""
    return DataSource(
        name="pudl_test",
        title="Pudl Test",
        description="Test dataset for the sandbox, thanks!",
        path="https://fake.link",
        license_raw=LICENSES["cc-by-4.0"],
        license_pudl=LICENSES["cc-by-4.0"],
    )


@pytest.fixture()
def test_settings():
    """Create temporary DOI settings file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        settings_file = Path(tmp_dir) / "settings.yaml"
        with open(settings_file, "w") as f:
            f.writelines(["fake_dataset:\n", "    sandbox_doi: null"])

        yield settings_file


@pytest.mark.asyncio
async def test_zenodo_workflow(
    session: aiohttp.ClientSession,
    upload_key: str,
    publish_key: str,
    test_settings: Path,
    test_files: dict[str, list[dict[str, str]]],
    deposition_metadata: DepositionMetadata,
    datasource: DataSource,
    mocker,
):
    """Test the entire zenodo client workflow."""

    def verify_files(expected, deposition: Deposition):
        deposition_files = {f.filename: f for f in deposition.files}
        for file_data in expected:
            # Verify that all expected files are in deposition
            assert file_data["path"].name in deposition_files

            # Download each file
            file_link = deposition_files[file_data["path"].name].links.download
            res = requests.get(
                file_link,
                params={"access_token": upload_key},
                timeout=10.0,
            )

            # Verify that contents of file are correct
            assert res.text.encode() == file_data["contents"]

    deposition_interface_args = {
        "data_source_id": "pudl_test",
        "session": session,
        "upload_key": upload_key,
        "publish_key": publish_key,
        "deposition_settings": test_settings,
        "dry_run": False,
        "sandbox": True,
        "auto_publish": True,
        "refresh_metadata": False,
    }

    class TestDownloader(AbstractDatasetArchiver):
        name = "Test Downloader"

        def __init__(self, resources, **kwargs):
            super().__init__(**kwargs)
            self.resources = resources

        async def get_resources(self):
            async def identity(x):
                return x

            for info in self.resources.values():
                yield identity(info)

    # Mock out creating deposition metadata with fake data source
    deposition_metadata_mock = mocker.MagicMock(return_value=deposition_metadata)
    mocker.patch(
        "pudl_archiver.orchestrator.DepositionMetadata.from_data_source",
        new=deposition_metadata_mock,
    )

    # Mock out creating datapackage with fake data source
    datasource_mock = mocker.MagicMock(return_value=datasource)
    mocker.patch(
        "pudl_archiver.frictionless.DataSource.from_id",
        new=datasource_mock,
    )

    # Create new deposition and add files

    v1_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["original"]
    }
    orchestrator = DepositionOrchestrator(
        **(
            deposition_interface_args
            | {
                "create_new": True,
                "downloader": TestDownloader(v1_resources, session=session),
            }
        )
    )
    v1_summary = await orchestrator.run()
    assert v1_summary.success

    v1_refreshed = await orchestrator.depositor.get_record(
        orchestrator.new_deposition.id_
    )
    verify_files(test_files["original"], v1_refreshed)

    # Update files
    v2_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["updated"]
    }
    orchestrator = DepositionOrchestrator(
        **(
            deposition_interface_args
            | {
                "downloader": TestDownloader(v2_resources, session=session),
            }
        )
    )

    # Should fail due to deleted file
    v2_summary = await orchestrator.run()
    assert not v2_summary.success

    # Wait for deleted deposition to propogate through
    time.sleep(1)

    # Disable test and re-run
    orchestrator.downloader.check_missing_files = False
    v2_summary = await orchestrator.run()
    assert v2_summary.success

    v2_refreshed = await orchestrator.depositor.get_record(
        orchestrator.new_deposition.id_
    )
    verify_files(test_files["updated"], v2_refreshed)

    # no updates to make, should not leave the conceptrecid pointing at a draft
    v3_summary = await orchestrator.run()
    assert isinstance(v3_summary, Unchanged)

    # unfortunately, it looks like Zenodo doesn't propagate deletion instantly - retry this a few times.
    latest_for_conceptrecid = await retry_async(
        orchestrator.depositor.get_deposition,
        args=[str(orchestrator.deposition.conceptrecid)],
        kwargs={"published_only": True},
        retry_on=(
            ZenodoClientException,
            aiohttp.ClientError,
            asyncio.TimeoutError,
            IndexError,
        ),
        retry_base_s=0.5,
    )
    assert latest_for_conceptrecid.id_ == v2_refreshed.id_

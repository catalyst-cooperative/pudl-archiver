"""Test zenodo client api."""
import os
import tempfile
import time
import unittest
from pathlib import Path

import aiohttp
import pytest
import pytest_asyncio
import requests
from dotenv import load_dotenv
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import LICENSES
from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors.depositor import Depositor
from pudl_archiver.depositors.zenodo.depositor import ZenodoDepositorInterface
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings
from pudl_archiver.zenodo.entities import (
    Deposition,
    DepositionCreator,
    DepositionMetadata,
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
                name="catalyst-cooperative", affiliation="Catalyst Cooperative"
            )
        ],
        description="Test dataset for the sandbox, thanks!",
        version="1.0.0",
        license="cc-zero",
        keywords=["test"],
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
            "updated": b"This file changes during the deposition update.",
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

                with Path.open(path, "wb") as f:
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
        settings_file = Path(tmp_dir) / "zenodo_doi.yaml"
        with Path.open(settings_file, "w") as f:
            f.writelines(["fake_dataset:\n", "    sandbox_doi: null"])

        yield tmp_dir


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
            links = deposition_files[file_data["path"].name].links
            res = requests.get(
                links.canonical,
                params={"access_token": upload_key},
                timeout=10.0,
            )
            if res.status_code == 500:
                res = requests.get(
                    links.download,
                    params={"access_token": upload_key},
                    timeout=10.0,
                )

            # Verify that contents of file are correct
            assert res.text.encode() == file_data["contents"]

    settings = RunSettings(
        dry_run=False,
        sandbox=True,
        auto_publish=True,
        refresh_metadata=False,
        initialize=True,
    )
    depositor = Depositor(
        interface=ZenodoDepositorInterface,
        dataset="pudl_test",
        session=session,
        settings=settings,
    )

    async def refresh_record_info(run_summary: RunSummary) -> Deposition:
        record_id = run_summary.record_url.path.rsplit("/", maxsplit=1)[1]
        return await depositor.get_deposition_by_id(record_id)

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
        "pudl_archiver.depositors.zenodo.DepositionMetadata.from_data_source",
        new=deposition_metadata_mock,
    )

    # Mock out creating datapackage with fake data source
    datasource_mock = mocker.MagicMock(return_value=datasource)
    mocker.patch(
        "pudl_archiver.frictionless.DataSource.from_id",
        new=datasource_mock,
    )

    # Mock settings path
    mocker.patch(
        "pudl_archiver.depositors.zenodo.depositor.importlib.resources.files",
        new=test_settings,
    )

    # Create new deposition and add files

    v1_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["original"]
    }
    v1_summary = await orchestrate_run(
        dataset="pudl_test",
        downloader=TestDownloader(v1_resources, session=session),
        depositor=depositor,
        session=session,
    )
    assert v1_summary.success

    v1_refreshed = await refresh_record_info(v1_summary)
    verify_files(test_files["original"], v1_refreshed)

    # the /records/ URL doesn't work until the record is published, but
    # deposit/ works from draft through publication
    assert str(v1_summary.record_url).replace("deposit", "records") == str(
        v1_refreshed.links.html
    )

    # Update files
    v2_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["updated"]
    }
    settings.initialize = False

    # Should fail due to deleted file
    downloader = TestDownloader(v2_resources, session=session)
    v2_summary = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        depositor=depositor,
        session=session,
    )
    assert not v2_summary.success

    # Wait for deleted deposition to propogate through
    time.sleep(1)

    # Disable test and re-run
    downloader.fail_on_missing_files = False
    v2_summary = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        depositor=depositor,
        session=session,
    )
    assert v2_summary.success

    v2_refreshed = await refresh_record_info(v2_summary)
    verify_files(test_files["updated"], v2_refreshed)

    # force a datapackage.json update
    with unittest.mock.patch(
        "pudl_archiver.orchestrator._datapackage_worth_changing",
        lambda *_args: True,
    ):
        v3_summary = await orchestrate_run(
            dataset="pudl_test",
            downloader=downloader,
            depositor=depositor,
            session=session,
        )
    assert len(v3_summary.file_changes) == 0

    v3_refreshed = await refresh_record_info(v3_summary)
    # no updates to make, should not leave the conceptdoi pointing at a draft
    v4_summary = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        depositor=depositor,
        session=session,
    )
    assert len(v4_summary.file_changes) == 0

    # legacy Zenodo API "get latest for concept DOI" endpoint is very slow to update,
    # but requesting the DOI directly updates quickly.
    res = requests.get(
        f"https://sandbox.zenodo.org/doi/{v3_refreshed.conceptdoi}",
        timeout=10.0,
    )
    assert str(v3_refreshed.id_) in res.text

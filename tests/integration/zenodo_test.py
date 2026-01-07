"""Test zenodo client api."""

import os
import tempfile
import time
import unittest
from pathlib import Path

import aiohttp
import pytest
import requests
from dotenv import load_dotenv
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import LICENSES

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.depositors.zenodo.entities import (
    Deposition,
    DepositionCreator,
    DepositionMetadata,
)
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings


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


@pytest.fixture()
async def session():
    """Create async http session."""
    async with aiohttp.ClientSession(raise_for_status=False) as session:
        yield session


@pytest.fixture()
def test_files():
    """Create files for testing in temporary directory."""
    file_data = {
        "unchanged_file.txt": {
            "original": b"This file should not change during deposition update.",
            "updated": b"This file should not change during deposition update.",
            "checksums": b"This file should not change during deposition update.",
        },
        "updated_file.txt": {
            "original": b"This file should updated during deposition update.",
            "updated": b"This file changes during the deposition update.",
            "checksums": b"This document is altered significantly over the course of the placement emendation.",
        },
        "deleted_file.txt": {
            "original": b"This file should deleted during deposition update.",
            "checksums": b"This file should deleted during deposition update.",
        },
        "created_file.txt": {
            "updated": b"This file should created during deposition update.",
            "checksums": b"This file should created during deposition update.",
        },
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        # Create directories
        file_paths = {}
        files = {}
        for dep_type in ["original", "updated", "checksums"]:
            file_paths[dep_type] = tmp_dir_path / dep_type
            file_paths[dep_type].mkdir()
            files[dep_type] = []

        # Loop through files and create them on local disk
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

        yield Path(tmp_dir)


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
    caplog,
):
    """Test the entire zenodo client workflow."""
    # Mock settings path
    settings_mock = mocker.MagicMock(return_value=test_settings)
    mocker.patch(
        "pudl_archiver.depositors.zenodo.depositor.importlib.resources.files",
        new=settings_mock,
    )

    settings = RunSettings(
        clobber_unchanged=True,
        auto_publish=False,
        refresh_metadata=False,
        initialize=True,
        depositor="zenodo",
        depositor_args={"sandbox": True},
    )

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
        "pudl_archiver.depositors.zenodo.entities.DepositionMetadata.from_data_source",
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

    # Test with auto publish off (should succeed, but not publish)
    v1_summary, v1_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=TestDownloader(v1_resources, session=session),
        run_settings=settings,
        session=session,
    )
    assert v1_summary.success
    assert v1_refreshed is None

    # Turn auto publish on for rest of run
    settings = settings.model_copy(update={"auto_publish": True})
    v1_summary, v1_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=TestDownloader(v1_resources, session=session),
        run_settings=settings,
        session=session,
    )
    assert v1_summary.success
    verify_files(test_files["original"], v1_refreshed.deposition)

    # the /records/ URL doesn't work until the record is published, but
    # deposit/ works from draft through publication
    assert str(v1_summary.record_url).replace("deposit", "records") == str(
        v1_refreshed.get_deposition_link()
    )

    # Update files
    settings.initialize = False

    v2_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["updated"]
    }

    # Should fail due to deleted file
    downloader = TestDownloader(v2_resources, session=session)
    downloader.fail_on_file_size_change = False
    downloader.fail_on_dataset_size_change = False
    v2_summary, v2_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        run_settings=settings,
        session=session,
    )
    assert not v2_summary.success

    # Wait for deleted deposition to propogate through
    time.sleep(1)

    # Disable test and re-run
    downloader.fail_on_missing_files = False
    v2_summary, v2_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        run_settings=settings,
        session=session,
    )
    assert v2_summary.success

    verify_files(test_files["updated"], v2_refreshed.deposition)

    vc_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["checksums"]
    }

    # Force a mismatched checksum for all files
    with unittest.mock.patch(
        "pudl_archiver.depositors.zenodo.depositor.ZenodoDraftDeposition.get_checksum",
        lambda *_args: "nonsense_checksum",
    ):
        downloader = TestDownloader(vc_resources, session=session)
        downloader.fail_on_file_size_change = False
        downloader.fail_on_dataset_size_change = False
        downloader.fail_on_missing_files = False
        with pytest.raises(RuntimeError, match=".*could not get checksums to match.*"):
            vc_summary, vc_refreshed = await orchestrate_run(
                dataset="pudl_test",
                downloader=downloader,
                run_settings=settings,
                session=session,
            )

    # Wait for deleted deposition to propogate through
    time.sleep(1)

    # re-run with normal checksums
    vc_summary, vc_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        run_settings=settings,
        session=session,
    )
    assert vc_summary.success

    verify_files(test_files["checksums"], vc_refreshed.deposition)
    ###

    # force a datapackage.json update
    with unittest.mock.patch(
        "pudl_archiver.archivers.validate._datapackage_changed",
        lambda *_args: True,
    ):
        v3_summary, v3_refreshed = await orchestrate_run(
            dataset="pudl_test",
            downloader=downloader,
            run_settings=settings,
            session=session,
        )
    assert len(v3_summary.file_changes) == 0

    # no updates to make, should not leave the conceptdoi pointing at a draft
    v4_summary, _ = await orchestrate_run(
        dataset="pudl_test",
        downloader=downloader,
        run_settings=settings,
        session=session,
    )
    assert len(v4_summary.file_changes) == 0
    assert caplog.records[-1].msg == "No changes detected, deleted draft."

    # legacy Zenodo API "get latest for concept DOI" endpoint is very slow to update,
    # but requesting the DOI directly updates quickly.
    res = requests.get(
        f"https://sandbox.zenodo.org/doi/{v3_refreshed.deposition.conceptdoi}",
        timeout=10.0,
    )
    assert str(v3_refreshed.deposition.id_) in res.text
    # Assert last draft actually deleted, getting DOI from end of record URL
    assert str(v4_summary.record_url).split("/")[-1] not in res.text

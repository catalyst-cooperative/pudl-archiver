"""Test zenodo client api."""
import os
from pathlib import Path
from datetime import datetime

import aiohttp
import pytest
import pytest_asyncio
import requests
import time
import tempfile
from dotenv import load_dotenv

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.zenodo.entities import DepositionMetadata, DepositionCreator


@pytest.fixture(name="dotenv")
def dotenv():
    load_dotenv()


@pytest.fixture(name="deposition_metadata")
def deposition_metadata():
    return DepositionMetadata(
        title="PUDL Test",
        creators=[DepositionCreator(name="catalyst-cooperative", affiliation="Catalyst Cooperative")],
        description="Test dataset for the sandbox, thanks!",
        version="1.0.0",
        license="cc-zero",
        keywords=["test"],
    )


@pytest.fixture(name="datapackage")
def get_datapackage():
    return DataPackage(
        name="pudl_test",
        title="PUDL Test",
        description="Test dataset for the sandbox, thanks!",
        keywords=[],
        contributors=[],
        sources=[],
        licenses=[],
        resources=[],
        created=datetime.now(),
    )


@pytest.fixture(name="upload_key")
def upload_key(dotenv):
    return os.environ["ZENODO_SANDBOX_TOKEN_UPLOAD"]


@pytest.fixture(name="publish_key")
def publish_key(dotenv):
    return os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]


@pytest_asyncio.fixture(name="session")
async def get_http_session():
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        yield session


@pytest_asyncio.fixture(name="test_files")
def create_test_files():
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


@pytest.fixture(name="test_settings")
def create_test_settings_file():
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
    datapackage: DataPackage,
    mocker,
):
    """Test the entire zenodo client workflow."""
    from pudl_archiver.zenodo.api_client import ZenodoClient
    client = ZenodoClient(test_settings, session, upload_key, publish_key, testing=True)

    # Mock out creating deposition metadata with fake data source
    deposition_metadata_mock = mocker.MagicMock(return_value=deposition_metadata)
    mocker.patch("pudl_archiver.zenodo.api_client.DepositionMetadata.from_data_source", new=deposition_metadata_mock)

    # Mock out creating datapackage with fake data source
    datapackage_mock = mocker.MagicMock(return_value=datapackage)
    mocker.patch("pudl_archiver.zenodo.api_client.DataPackage.from_filelist", new=datapackage_mock)

    # Create new deposition and add files
    async with client.deposition_interface("pudl_test", initialize=True) as interface:
        resources = {
            file_data["path"].name: ResourceInfo(local_path=file_data["path"], partitions={})
            for file_data in test_files["original"]
        }
        await interface.add_files(resources)

    # Wait before trying to access newly created deposition
    time.sleep(1.0)
    async with client.deposition_interface("pudl_test") as interface:
        # Get files from first version of deposition
        for file_data in test_files["original"]:
            # Verify that all expected files are in deposition
            assert file_data["path"].name in interface.deposition_files

            # Download each file
            file_link = interface.deposition_files[file_data["path"].name].links.download
            res = requests.get(file_link, params={"access_token": upload_key})

            # Verify that contents of file are correct
            assert res.text.encode() == file_data["contents"]

        # Update files
        resources = {
            file_data["path"].name: ResourceInfo(local_path=file_data["path"], partitions={})
            for file_data in test_files["updated"]
        }
        await interface.add_files(resources)

    # Wait before trying to access newly created deposition
    time.sleep(1.0)
    async with client.deposition_interface("pudl_test") as interface:
        # Get files from updated version of deposition
        for file_data in test_files["updated"]:
            assert file_data["path"].name in interface.deposition_files
            file_link = interface.deposition_files[file_data["path"].name].links.download
            res = requests.get(file_link, params={"access_token": upload_key})
            assert res.text.encode() == file_data["contents"]

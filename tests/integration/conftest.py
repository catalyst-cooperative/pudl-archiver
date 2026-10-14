"""Fixtures shared by the integration tests of the Zenodo sandbox."""

import tempfile
from pathlib import Path

import aiohttp
import pytest
from dotenv import load_dotenv

from pudl_archiver.depositors.zenodo.entities import (
    DepositionCreator,
    DepositionMetadata,
)
from pudl_archiver.metadata.constants import LICENSES


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
async def session():
    """Create async http session."""
    async with aiohttp.ClientSession(raise_for_status=False) as session:
        yield session


@pytest.fixture()
def datasource():
    """Create fake datasource for testing."""
    return {
        "name": "pudl_test",
        "title": "Pudl Test",
        "description": "Test dataset for the sandbox, thanks!",
        "path": "https://fake.link",
        "license_raw": LICENSES["cc-by-4.0"],
        "contributors": [
            {"title": "Catalyst Cooperative", "organization": "Catalyst Cooperative"}
        ],
    }


@pytest.fixture()
def test_settings():
    """Create temporary DOI settings file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        settings_file = Path(tmp_dir) / "zenodo_doi.yaml"
        with Path.open(settings_file, "w") as f:
            f.writelines(["fake_dataset:\n", "    sandbox_doi: null"])

        yield Path(tmp_dir)


@pytest.fixture()
def zenodo_sandbox(mocker, deposition_metadata, datasource, test_settings):
    """Archive the fake dataset 'pudl_test' to the sandbox, with its own settings."""
    mocker.patch(
        "pudl_archiver.depositors.zenodo.depositor.importlib.resources.files",
        new=mocker.MagicMock(return_value=test_settings),
    )
    mocker.patch(
        "pudl_archiver.depositors.zenodo.entities.DepositionMetadata.from_data_source",
        return_value=deposition_metadata,
    )
    mocker.patch(
        "pudl_archiver.frictionless.get_pudl_sources",
        return_value={"pudl_test": datasource},
    )

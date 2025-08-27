"""Pytest configuration module."""

import zipfile
from datetime import datetime
from pathlib import Path

import pytest

from pudl_archiver.frictionless import DataPackage


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
        created=str(datetime.now()),
        version="1.0.0",
    )


@pytest.fixture()
def bad_zipfile(tmp_path):
    """Create a fake bad zipfile as a temp file."""
    zip_path = tmp_path / "bad.zip"
    with Path.open(zip_path, "wb") as archive:
        archive.write(b"Fake non-zipfile data")

    yield zip_path


@pytest.fixture()
def fixed_bad_zipfile(tmp_path):
    """Create a fixed version of 'bad.zip' as a temp file."""
    zip_path = tmp_path / "bad.zip"
    with (
        zipfile.ZipFile(zip_path, "w") as archive,
        archive.open("test.txt", "w") as file,
    ):
        file.write(b"Test fixed bad zipfile")

    yield zip_path


@pytest.fixture()
def good_zipfile(tmp_path):
    """Create a fake good zipfile in temporary directory."""
    zip_path = tmp_path / "good.zip"
    with (
        zipfile.ZipFile(zip_path, "w") as archive,
        archive.open("test.txt", "w") as file,
    ):
        file.write(b"Test good zipfile")

    yield zip_path

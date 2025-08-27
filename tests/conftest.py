"""Pytest configuration module."""

import tempfile
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
def bad_zipfile():
    """Create a fake bad zipfile as a temp file."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "bad.zip"
        with Path.open(zip_path, "wb") as archive:
            archive.write(b"Fake non-zipfile data")

        yield zip_path


@pytest.fixture()
def good_zipfile():
    """Create a fake good zipfile in temporary directory."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "good.zip"
        with (
            zipfile.ZipFile(zip_path, "w") as archive,
            archive.open("test.txt", "w") as file,
        ):
            file.write(b"Test good zipfile")

        yield zip_path

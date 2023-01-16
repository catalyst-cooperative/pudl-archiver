"""Test archiver abstract base class."""
import io
import tempfile
from zipfile import ZipFile
import pytest
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


@pytest.fixture(name="bad_zipfile")
def create_bad_zipfile():
    """Create a fake bad zipfile in temporary directory."""
    with tempfile.TemporaryFile() as file:
        file.write(b"Fake non-zipfile data")
        yield file


@pytest.fixture(name="good_zipfile")
def create_good_zipfile():
    """Create a fake bad zipfile in temporary directory."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "test.zip"
        with ZipFile(zip_path, "w") as archive:
            with archive.open("test.txt", "w") as file:
                file.write(b"Test good zipfile")

        yield zip_path


class TestArchiver(AbstractDatasetArchiver):
    """Class to test AbstractDatasetArchiver."""

    name = "test_archiver"

    async def get_resources(self) -> ArchiveAwaitable:
        """Create fake resources."""
        pass


@pytest.mark.asyncio
async def test_download_zipfile(mocker, bad_zipfile, good_zipfile):
    """Test download zipfile.

    Tests the zipfile validation, does not actually download any files.
    """
    # Patch download_file
    mocker.patch("pudl_archiver.archivers.classes.AbstractDatasetArchiver.download_file")

    # Initialize TestArchiver class
    archiver = TestArchiver(None, None)

    url = "www.fake.url.com"
    with pytest.raises(RuntimeError, match=f"Failed to download valid zipfile from {url}"):
        await archiver.download_zipfile(url, bad_zipfile)

    assert not await archiver.download_zipfile(url, good_zipfile)

    assert not await archiver.download_zipfile(url, good_zipfile.open(mode="rb"))

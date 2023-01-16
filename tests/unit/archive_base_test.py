"""Test archiver abstract base class."""
import io
import re
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ArchiveAwaitable


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


@pytest.fixture(name="file_data")
def create_test_file():
    """Create test file data for download_file test."""
    return b"Junk test file data"


class MockArchiver(AbstractDatasetArchiver):
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
    mocker.patch(
        "pudl_archiver.archivers.classes.AbstractDatasetArchiver.download_file"
    )

    # Initialize MockArchiver class
    archiver = MockArchiver(None, None)

    url = "www.fake.url.com"
    with pytest.raises(
        RuntimeError, match=f"Failed to download valid zipfile from {url}"
    ):
        await archiver.download_zipfile(url, bad_zipfile)

    assert not await archiver.download_zipfile(url, good_zipfile)

    assert not await archiver.download_zipfile(url, good_zipfile.open(mode="rb"))


@pytest.mark.asyncio
async def test_download_file(mocker, file_data):
    """Test download_file.

    Tests that expected data is written to file on disk or in memory. Doesn't
    actually download any files.
    """
    # Initialize MockArchiver class
    archiver = MockArchiver(None, None)

    session_mock = mocker.MagicMock(name="session_mock")
    archiver.session = session_mock

    # Set return value
    session_mock.get.return_value.__aenter__.return_value.read.return_value = file_data

    # Prepare args
    url = "www.fake.url.com"
    file = io.BytesIO()

    # Call method
    await archiver.download_file(url, file)

    session_mock.get.assert_called_once_with(url)

    assert file.getvalue() == file_data

    # Rerun test with path to file
    session_mock.get.reset_mock()
    with tempfile.TemporaryDirectory() as path:
        file_path = Path(path) / "test"
        await archiver.download_file(url, file_path)

        session_mock.get.assert_called_once_with(url)
        file = file_path.open("rb")
        assert file.read() == file_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "html,pattern,links",
    [
        (
            """
        <!DOCTYPE html>
        <html>
            <body>
                <h1>random heading</h1>
                <p>paragraph</p>
                <a href='https://www.fake.link.com/test_2019.zip'>Text</a>
                <div>
                    <a href='https://www.fake.link.com/test_2020.zip'>Text</a>
                </div>
                <a href='https://www.fake.link.com/not/a/match/'>Text</a>
            </body>
        </html>
        """,
            re.compile(r"test_\d{4}.zip"),
            [
                "https://www.fake.link.com/test_2019.zip",
                "https://www.fake.link.com/test_2020.zip",
            ],
        ),
        (
            """
        <!DOCTYPE html>
        <html>
            <body>
                <h1>random heading</h1>
                <div>
                    <p>paragraph</p>
                <a href='https://www.fake.link.com/test_2019.zip'>Text</a>
                <div>
                    <a href='https://www.fake.link.com/test_2020.zip'>Text</a>
                </div>
                <a href='https://www.fake.link.com/not/a/match/'>Text</a>
            </body>
        </html>
        """,
            None,
            [
                "https://www.fake.link.com/test_2019.zip",
                "https://www.fake.link.com/test_2020.zip",
                "https://www.fake.link.com/not/a/match/",
            ],
        ),
    ],
)
async def test_get_hyperlinks(html, pattern, links, request):
    """Test get hyperlinks function."""
    # Initialize MockArchiver class
    archiver = MockArchiver(None, None)

    mocker = request.getfixturevalue("mocker")

    session_mock = mocker.MagicMock(name="session_mock")
    archiver.session = session_mock

    # Set return value
    session_mock.get.return_value.__aenter__.return_value.text.return_value = html

    found_links = await archiver.get_hyperlinks("fake_url", pattern)
    assert set(found_links) == set(links)

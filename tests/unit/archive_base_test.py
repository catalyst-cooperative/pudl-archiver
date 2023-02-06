"""Test archiver abstract base class."""
import io
import re
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest
from aiohttp import ClientError, ClientSession

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ArchiveAwaitable


@pytest.fixture()
def bad_zipfile():
    """Create a fake bad zipfile as a temp file."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "test.zip"
        with open(zip_path, "wb") as archive:
            archive.write(b"Fake non-zipfile data")

        yield zip_path


@pytest.fixture()
def good_zipfile():
    """Create a fake good zipfile in temporary directory."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "test.zip"
        with ZipFile(zip_path, "w") as archive:
            with archive.open("test.txt", "w") as file:
                file.write(b"Test good zipfile")

        yield zip_path


@pytest.fixture()
def file_data():
    """Create test file data for download_file test."""
    return b"Junk test file data"


class MockArchiver(AbstractDatasetArchiver):
    """Class to test AbstractDatasetArchiver."""

    name = "test_archiver"

    async def get_resources(self) -> ArchiveAwaitable:
        """Create fake resources."""
        pass


@pytest.fixture()
def html_docs():
    """Define html docs for parser test."""
    return {
        "simple": """<!doctype html>
        <html>
            <body>
                <h1>random heading</h1>
                <p>paragraph</p>
                <a href='https://www.fake.link.com/test_2019.zip'>text</a>
                <div>
                    <a href='https://www.fake.link.com/test_2020.zip'>text</a>
                </div>
                <a href='https://www.fake.link.com/not/a/match/'>text</a>
            </body>
        </html>
        """,
    }


@pytest.mark.asyncio
async def test_download_zipfile(mocker, bad_zipfile, good_zipfile):
    """Test download zipfile.

    Tests the zipfile validation, does not actually download any files.
    """
    print(bad_zipfile)
    # Patch download_file
    mocked_download_file = mocker.patch(
        "pudl_archiver.archivers.classes.AbstractDatasetArchiver.download_file"
    )

    # Initialize MockArchiver class
    archiver = MockArchiver(None)

    url = "www.fake.url.com"
    with pytest.raises(
        RuntimeError, match=f"Failed to download valid zipfile from {url}"
    ):
        await archiver.download_zipfile(url, bad_zipfile, retries=4)
        # though - if we retry 4 times, technically shouldn't we have called 5?
    assert mocked_download_file.call_count == 4

    # Test function succeeds with path to zipfile
    assert not await archiver.download_zipfile(url, good_zipfile)
    assert mocked_download_file.call_count == 5

    # Test function succeeds with file object
    assert not await archiver.download_zipfile(url, good_zipfile.open(mode="rb"))
    assert mocked_download_file.call_count == 6


@pytest.mark.asyncio
async def test_download_file(mocker, file_data):
    """Test download_file.

    Tests that expected data is written to file on disk or in memory. Doesn't
    actually download any files.
    """
    # Initialize MockArchiver class
    archiver = MockArchiver(None)

    session_mock = mocker.AsyncMock(name="session_mock")
    archiver.session = session_mock

    # Set return value
    response_mock = mocker.AsyncMock()
    response_mock.read = mocker.AsyncMock(return_value=file_data)
    session_mock.get = mocker.AsyncMock(return_value=response_mock)

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
    "docname,pattern,links",
    [
        (
            "simple",
            re.compile(r"test_\d{4}.zip"),
            [
                "https://www.fake.link.com/test_2019.zip",
                "https://www.fake.link.com/test_2020.zip",
            ],
        ),
        (
            "simple",
            None,
            [
                "https://www.fake.link.com/test_2019.zip",
                "https://www.fake.link.com/test_2020.zip",
                "https://www.fake.link.com/not/a/match/",
            ],
        ),
    ],
)
async def test_get_hyperlinks(docname, pattern, links, request, html_docs):
    """Test get hyperlinks function."""
    # Get desired html doc
    html = html_docs[docname]

    # Initialize MockArchiver class
    archiver = MockArchiver(None)

    mocker = request.getfixturevalue("mocker")

    session_mock = mocker.AsyncMock(name="session_mock", spec=ClientSession)
    archiver.session = session_mock

    # Set return value
    response_mock = mocker.AsyncMock()
    response_mock.text = mocker.AsyncMock(return_value=html)
    session_mock.get = mocker.AsyncMock(return_value=response_mock)

    found_links = await archiver.get_hyperlinks("fake_url", pattern)
    assert set(found_links) == set(links)


@pytest.mark.asyncio
async def test_retries(mocker):
    # Initialize MockArchiver class
    archiver = MockArchiver(None)
    session_mock = mocker.Mock(name="session_mock")
    archiver.session = session_mock
    sleep_mock = mocker.AsyncMock()
    mocker.patch("asyncio.sleep", sleep_mock)
    session_mock.get = mocker.Mock(side_effect=ClientError("test error"))

    with pytest.raises(ClientError):
        await archiver.download_file("foo", io.BytesIO())

    assert session_mock.get.call_count == 5
    assert sleep_mock.call_count == 4
    sleep_mock.assert_has_calls([mocker.call(x) for x in [2, 4, 8, 16]])

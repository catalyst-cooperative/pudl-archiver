"""Test archiver abstract base class."""
import copy
import io
import re
import tempfile
import zipfile
from pathlib import Path

import pytest
from aiohttp import ClientSession
from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ArchiveAwaitable
from pudl_archiver.archivers.validate import ValidationTestResult
from pudl_archiver.frictionless import Resource, ResourceInfo


@pytest.fixture()
def bad_zipfile():
    """Create a fake bad zipfile as a temp file."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "test.zip"
        with Path.open(zip_path, "wb") as archive:
            archive.write(b"Fake non-zipfile data")

        yield zip_path


@pytest.fixture()
def good_zipfile():
    """Create a fake good zipfile in temporary directory."""
    with tempfile.TemporaryDirectory() as path:
        zip_path = Path(path) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as archive, archive.open(
            "test.txt", "w"
        ) as file:
            file.write(b"Test good zipfile")

        yield zip_path


@pytest.fixture()
def file_data():
    """Create test file data for download_file test."""
    return b"Junk test file data"


class MockArchiver(AbstractDatasetArchiver):
    """Class to test AbstractDatasetArchiver."""

    name = "test_archiver"

    def __init__(self, test_results, **kwargs):
        self.test_results = test_results
        self.file_validations = {}
        super().__init__(session=None, **kwargs)

    async def get_resources(self) -> ArchiveAwaitable:
        """Create fake resources."""
        pass

    def dataset_validate_archive(
        self, baseline_datapackage, new_datapackage, resources
    ) -> list[ValidationTestResult]:
        """Return fake test results."""
        return self.test_results


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
@pytest.mark.parametrize(
    "concurrency_limit,directory_per_resource_chunk,download_paths",
    [
        (1, True, ["path0", "path1", "path2", "path3", "path4"]),
        (1, False, ["path0", "path0", "path0", "path0", "path0"]),
        (5, True, ["path0", "path0", "path0", "path0", "path0"]),
        (2, True, ["path0", "path0", "path1", "path1", "path2"]),
    ],
)
async def test_resource_chunks(
    concurrency_limit, directory_per_resource_chunk, download_paths, mocker
):
    """AbstractDatasetArchiver should be able to download resources in chunks."""

    class MockArchiver(AbstractDatasetArchiver):
        name = "mock"

        def __init__(self, concurrency_limit, directory_per_resource_chunk):
            self.concurrency_limit = concurrency_limit
            self.directory_per_resource_chunk = directory_per_resource_chunk
            super().__init__(None)

        async def get_resources(self):
            for i, _ in enumerate(download_paths):
                yield self.get_resource(i)

        async def get_resource(self, i):
            return ResourceInfo(
                local_path=Path(self.download_directory), partitions={"idx": i}
            )

    tmpdir_mock = mocker.Mock(side_effect=[Path(f"path{i}") for i in range(6)])
    mocker.patch(
        "pudl_archiver.archivers.classes.tempfile.TemporaryDirectory",
        new=tmpdir_mock,
    )

    # Mock out file validations
    mocker.patch("pudl_archiver.archivers.classes.validate.validate_filetype")
    mocker.patch("pudl_archiver.archivers.classes.validate.validate_file_not_empty")
    mocker.patch("pudl_archiver.archivers.classes.validate.validate_zip_layout")

    # Initialize MockArchiver class
    archiver = MockArchiver(concurrency_limit, directory_per_resource_chunk)
    async for name, resource in archiver.download_all_resources():
        assert download_paths[resource.partitions["idx"]] == name


@pytest.mark.asyncio
async def test_download_zipfile(mocker, bad_zipfile, good_zipfile):
    """Test download zipfile.

    Tests the zipfile validation, does not actually download any files.
    """
    # Patch download_file
    mocked_download_file = mocker.patch(
        "pudl_archiver.archivers.classes.AbstractDatasetArchiver.download_file"
    )

    # Initialize MockArchiver class
    archiver = MockArchiver(None)

    url = "https://www.fake.url.com"
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
    url = "https://www.fake.url.com"
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
async def test_download_and_zip_file(mocker, file_data):
    """Test download_and_zip_file.

    Tests that expected data is written to file on disk in a zipfile.
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
    url = "https://www.fake.url.com"

    # Run test with path to temp dir
    with tempfile.TemporaryDirectory() as path:
        file_path = str(Path(path) / "test.csv")
        archive_path = str(Path(path) / "test.zip")

        await archiver.download_and_zip_file(url, file_path, archive_path)
        # Assert that the zipfile at archive_path contains a file at file_path
        session_mock.get.assert_called_once_with(url)
        with zipfile.ZipFile(archive_path) as zf:
            zipped_file = zf.open(file_path)
            assert zipped_file.read() == file_data


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


@pytest.mark.parametrize(
    "test_results,success",
    [
        (
            [
                ValidationTestResult(name="test0", description="test0", success=True),
                ValidationTestResult(name="test1", description="test1", success=True),
                ValidationTestResult(name="test2", description="test2", success=True),
            ],
            True,
        ),
        (
            [
                ValidationTestResult(name="test0", description="test0", success=True),
                ValidationTestResult(name="test1", description="test1", success=True),
                ValidationTestResult(name="test2", description="test2", success=False),
            ],
            False,
        ),
        (
            [
                ValidationTestResult(name="test0", description="test0", success=True),
                ValidationTestResult(name="test1", description="test1", success=True),
                ValidationTestResult(
                    name="test2",
                    description="test2",
                    success=False,
                    required_for_run_success=False,
                ),
            ],
            True,
        ),
    ],
)
def test_generate_summary(datapackage, test_results, success):
    """Test that validate_archive method handles dataset specific tests properly."""
    archiver = MockArchiver(test_results)
    assert (
        archiver.generate_summary(datapackage, datapackage, "resources").success
        == success
    )


@pytest.mark.parametrize(
    "baseline_resources,new_resources,success",
    [
        (
            [
                Resource(
                    name="resource0",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
                Resource(
                    name="resource1",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
                Resource(
                    name="resource1",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
                Resource(
                    name="resource2",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
            ],
            True,
        ),
        (
            [
                Resource(
                    name="resource0",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
                Resource(
                    name="resource1",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.example.com",
                    remote_url="https://www.example.com",
                    title="",
                    parts={},
                    mediatype="",
                    format="",
                    bytes=0,
                    hash="",
                ),
            ],
            False,
        ),
    ],
)
def test_check_missing_files(datapackage, baseline_resources, new_resources, success):
    """Test the ``_check_missing_files`` validation test."""
    archiver = MockArchiver(None)

    baseline_datapackage = copy.deepcopy(datapackage)
    baseline_datapackage.resources = baseline_resources

    new_datapackage = copy.deepcopy(datapackage)
    new_datapackage.resources = new_resources

    validation_result = archiver._check_missing_files(
        baseline_datapackage, new_datapackage
    )
    assert validation_result.success == success


def test_year_filter():
    archiver = MockArchiver(None, only_years=[2020, 2022])
    assert archiver.valid_year(2020)
    assert not archiver.valid_year(2021)
    assert archiver.valid_year(2022)
    assert archiver.valid_year("2022")

    archiver_no_filter = MockArchiver(None)
    assert archiver_no_filter.valid_year(2021)

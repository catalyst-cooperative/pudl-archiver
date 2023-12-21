"""Test archiver abstract base class."""
import copy
import io
import logging
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


def _resource_w_size(name: str, size: int):
    """Create resource with variable size for use in tests."""
    return Resource(
        name=name,
        path=f"https://www.example.com/{name}",
        remote_url="https://www.example.com",
        title="",
        parts={},
        mediatype="",
        format="",
        bytes=size,
        hash="",
    )


def _resource_w_parts(name: str, parts: dict):
    """Create resource with variable size for use in tests."""
    return Resource(
        name=name,
        path=f"https://www.example.com/{name}",
        remote_url="https://www.example.com",
        title="",
        parts=parts,
        mediatype="",
        format="",
        bytes=10,
        hash="",
    )


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
    "baseline_resources,new_resources,success",
    [
        (
            [_resource_w_size("resource0", 0), _resource_w_size("resource1", 0)],
            [
                _resource_w_size("resource0", 0),
                _resource_w_size("resource1", 0),
                _resource_w_size("resource2", 0),
            ],
            True,
        ),
        (
            [_resource_w_size("resource0", 0), _resource_w_size("resource1", 0)],
            [
                _resource_w_size("resource0", 0),
            ],
            False,
        ),
    ],
    ids=["create_file", "delete_file"],
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


@pytest.mark.parametrize(
    "baseline_resources,new_resources,success",
    [
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 20),
                _resource_w_size("resource1", 10),
            ],
            False,
        ),
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 11),
                _resource_w_size("resource1", 9),
            ],
            True,
        ),
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 10),
            ],
            True,
        ),
        (
            [
                _resource_w_size("resource0", 10),
            ],
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            True,
        ),
        (
            None,
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            True,
        ),
    ],
    ids=[
        "file_too_big",
        "file_change_acceptable",
        "file_deleted",
        "file_created",
        "no_base_datapackage",
    ],
)
def test_check_file_size(datapackage, baseline_resources, new_resources, success):
    """Test the ``_check_file_size`` validation test."""
    archiver = MockArchiver(None)

    if baseline_resources is None:
        baseline_datapackage = None
    else:
        baseline_datapackage = copy.deepcopy(datapackage)
        baseline_datapackage.resources = baseline_resources

    new_datapackage = copy.deepcopy(datapackage)
    new_datapackage.resources = new_resources

    validation_result = archiver._check_file_size(baseline_datapackage, new_datapackage)
    assert validation_result.success == success


@pytest.mark.parametrize(
    "baseline_resources,new_resources,success",
    [
        (
            [
                _resource_w_size("resource0", 0),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            True,
        ),
    ],
)
def test_check_zero_file_size(
    datapackage, baseline_resources, new_resources, success, caplog
):
    """Test the ``_check_file_size`` validation test."""
    archiver = MockArchiver(None)

    baseline_datapackage = copy.deepcopy(datapackage)
    baseline_datapackage.resources = baseline_resources

    new_datapackage = copy.deepcopy(datapackage)
    new_datapackage.resources = new_resources

    with caplog.at_level(logging.WARN):
        validation_result = archiver._check_file_size(
            baseline_datapackage, new_datapackage
        )
    assert validation_result.success == success
    assert "Original file size was zero" in caplog.text


@pytest.mark.parametrize(
    "baseline_resources,new_resources,success",
    [
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 20),
                _resource_w_size("resource1", 10),
            ],
            False,
        ),
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 11),
                _resource_w_size("resource1", 9),
            ],
            True,
        ),
        (
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            [
                _resource_w_size("resource0", 10),
            ],
            False,
        ),
        (
            [
                _resource_w_size("resource0", 10),
            ],
            [
                _resource_w_size("resource0", 10),
            ],
            True,
        ),
        (
            None,
            [
                _resource_w_size("resource0", 10),
                _resource_w_size("resource1", 10),
            ],
            True,
        ),
    ],
    ids=[
        "increase_too_big",
        "file_change_no_overall_change",
        "decrease_too_big",
        "no_change",
        "no_base_datapackage",
    ],
)
def test_check_dataset_size(datapackage, baseline_resources, new_resources, success):
    """Test the ``_check_dataset_size`` validation test."""
    archiver = MockArchiver(None)

    if baseline_resources is None:
        baseline_datapackage = None
    else:
        baseline_datapackage = copy.deepcopy(datapackage)
        baseline_datapackage.resources = baseline_resources

    new_datapackage = copy.deepcopy(datapackage)
    new_datapackage.resources = new_resources

    validation_result = archiver._check_dataset_size(
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


@pytest.mark.parametrize(
    "new_resources,success,notes",
    [
        (
            [
                _resource_w_parts(
                    "resource0",
                    {"year_quarter": ["1995q1", "1995q2", "1995q3", "1995q4"]},
                ),
                _resource_w_parts(
                    "resource1", {"year_quarter": ["1996q1", "1996q2", "1996q3"]}
                ),
            ],
            True,
            "All tested partitions",
        ),
        (
            [
                _resource_w_parts(
                    "resource0",
                    {"year_month": [f"1995-{month:02d}" for month in range(2, 13)]},
                ),
                _resource_w_parts(
                    "resource1",
                    {"year_month": [f"1996-{month:02d}" for month in range(1, 8)]},
                ),
            ],
            True,
            "All tested partitions",
        ),
        (
            [
                _resource_w_parts(
                    "resource0", {"year_quarter": ["1995q1", "1995q3", "1995q4"]}
                ),
                _resource_w_parts(
                    "resource1", {"year_quarter": ["1996q1", "1996q2", "1996q3"]}
                ),
            ],
            False,
            "not continuous",
        ),
        (
            [
                _resource_w_parts(
                    "resource0",
                    {
                        "year_quarter": [
                            "1995q1",
                            "1995q1",
                            "1995q2",
                            "1995q3",
                            "1995q4",
                        ]
                    },
                )
            ],
            False,
            "duplicate time periods",
        ),
        (
            [
                _resource_w_parts("resource0", {"year_quarter": "1995q1"}),
                _resource_w_parts("resource1", {"year_quarter": "1995q2"}),
            ],
            True,
            "All tested partitions",
        ),
        (
            [
                _resource_w_parts("resource0", {"year": "1995"}),
                _resource_w_parts("resource1", {"year": "1996"}),
            ],
            True,
            "not configured for this test",
        ),
        (
            [
                _resource_w_parts(
                    "resource1", {"year_quarter": ["1996q1", "1996q2", "1996q3"]}
                ),
                _resource_w_parts(
                    "resource0",
                    {"year_quarter": ["1995q1", "1995q2", "1995q3", "1995q4"]},
                ),
            ],
            True,
            "All tested partitions",
        ),
        (
            [
                _resource_w_parts(
                    "resource0", {"year_quarter": ["1995q1", "1995q2"], "form": "junk"}
                ),
                _resource_w_parts(
                    "resource1",
                    {"year_quarter": ["1995q3", "1995q4"], "form": "random"},
                ),
            ],
            True,
            "All tested partitions",
        ),
        (
            [
                _resource_w_parts(
                    "resource0",
                    {"year_quarter": ["1995q1", "1995q2"], "year_month": "1995-01"},
                ),
                _resource_w_parts(
                    "resource1",
                    {"year_quarter": ["1995q3", "1995q4"], "year_month": "1995-06"},
                ),
            ],
            False,
            "more than one partition",
        ),
    ],
    ids=[
        "all_expected_quarter_files",
        "all_expected_month_files",
        "missing_partition",
        "duplicated_partition",
        "partitions_not_lists",
        "partition_not_tested",
        "out_of_order_files",
        "multiple_partitions",
        "multiple_tested_partitions",
    ],
)
def test_check_data_continuity(datapackage, new_resources, success, notes):
    """Test the dataset archiving valiation for epacems."""
    archiver = MockArchiver(None)
    new_datapackage = copy.deepcopy(datapackage)
    new_datapackage.resources = new_resources
    validation = archiver._check_data_continuity(new_datapackage)
    assert validation.success == success
    assert notes in validation.notes[0]  # Check exact success/fail reason

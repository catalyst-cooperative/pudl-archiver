"""Test fsspec based depositor backend."""

import json
import tempfile
from pathlib import Path

import pytest
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import LICENSES

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings


@pytest.fixture()
def test_files():
    """Create files for testing in temporary directory."""
    file_data = {
        "unchanged_file.txt": {
            "original": b"This file should not change during deposition update.",
            "updated": b"This file should not change during deposition update.",
        },
        "updated_file.txt": {
            "original": b"This file should updated during deposition update.",
            "updated": b"This file changes during the deposition update.",
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

                with Path.open(path, "wb") as f:
                    f.write(contents)

        yield files


@pytest.fixture()
def datasource(mocker):
    """Create fake datasource for testing."""
    datasource = DataSource(
        name="pudl_test",
        title="Pudl Test",
        description="Test dataset for the sandbox, thanks!",
        path="https://fake.link",
        license_raw=LICENSES["cc-by-4.0"],
        license_pudl=LICENSES["cc-by-4.0"],
    )
    # Mock out creating datapackage with fake data source
    datasource_mock = mocker.MagicMock(return_value=datasource)
    mocker.patch(
        "pudl_archiver.frictionless.DataSource.from_id",
        new=datasource_mock,
    )


@pytest.mark.asyncio
async def test_retry_run(
    good_zipfile,
    bad_zipfile,
    fixed_bad_zipfile,
    tmp_path,
    datasource: DataSource,
    mocker,
):
    """Test a an archiver retry run using fsspec depositor.

    This test will creates a mock archiver that will 'download'
    two zipfiles. One will be well formatted, while the other won't.
    This will lead to a failed run that we will retry. On the second
    run, it will retry the failed partition, and we return a fixed
    zipfile, so the archiver should succeed and publish the results.
    """
    deposition_path = tmp_path / "deposition"
    deposition_path.mkdir()

    settings = RunSettings(
        clobber_unchanged=True,
        auto_publish=True,
        refresh_metadata=False,
        initialize=True,
        depositor="fsspec",
        depositor_args={"deposition_path": str(deposition_path)},
    )
    retry_part = {"part": "retry_part"}
    ok_part = {"part": "ok_part"}

    class TestDownloader(AbstractDatasetArchiver):
        name = "Test Downloader"

        def __init__(self, fail_part: bool, **kwargs):
            super().__init__(**kwargs)
            self.fail_part = fail_part
            self.good_downloaded = False

        async def get_resources(self):
            if self.fail_part:
                yield self.get_zipfile(bad_zipfile, parts=retry_part), retry_part
            else:
                yield self.get_zipfile(fixed_bad_zipfile, parts=retry_part), retry_part

            yield self.get_zipfile(good_zipfile, parts=ok_part), ok_part

        async def get_zipfile(self, zip_path, parts):
            # Check if we're downloading 'good.zip'. This shouldn't happen in the retry run
            if zip_path.name == "good.zip":
                self.good_downloaded = True
            return ResourceInfo(local_path=zip_path, partitions=parts)

    v1_summary, _ = await orchestrate_run(
        dataset="pudl_test",
        downloader=TestDownloader(fail_part=True, session="session"),
        run_settings=settings,
        session="session",
    )
    with (tmp_path / "run_summary.json").open("w") as f:
        f.write(json.dumps([v1_summary.model_dump()], indent=2))

    assert retry_part == v1_summary.failed_partitions["bad.zip"]
    assert ok_part == v1_summary.successful_partitions["good.zip"]
    assert not (deposition_path / "published" / "bad.zip").exists()
    assert not (deposition_path / "published" / "good.zip").exists()
    assert (deposition_path / "workspace" / "bad.zip").exists()
    assert (deposition_path / "workspace" / "good.zip").exists()
    assert not v1_summary.success

    settings.retry_run = str(tmp_path / "run_summary.json")
    v2_downloader = TestDownloader(fail_part=False, session="session")
    v2_summary, _ = await orchestrate_run(
        dataset="pudl_test",
        downloader=v2_downloader,
        run_settings=settings,
        session="session",
        failed_partitions=v1_summary.failed_partitions,
        successful_partitions=v1_summary.successful_partitions,
    )
    assert not v2_downloader.good_downloaded
    assert v2_summary.success
    assert (deposition_path / "published" / "bad.zip").exists()
    assert (deposition_path / "published" / "good.zip").exists()


@pytest.mark.asyncio
async def test_fsspec_depositor(
    test_files: dict[str, list[dict[str, str]]],
    datasource: DataSource,
    mocker,
    tmp_path,
):
    """Test fsspec depositor backend."""
    settings = RunSettings(
        clobber_unchanged=True,
        auto_publish=False,
        refresh_metadata=False,
        initialize=True,
        depositor="fsspec",
        depositor_args={"deposition_path": str(tmp_path)},
    )

    def verify_files(expected, deposition_path: Path):
        for file_data in expected:
            assert (deposition_path / file_data["path"].name).read_bytes() == file_data[
                "contents"
            ]

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

    # Create new deposition and add files
    v1_resources = {
        file_data["path"].name: ResourceInfo(
            local_path=file_data["path"], partitions={}
        )
        for file_data in test_files["original"]
    }

    # Turn auto publish on for rest of run
    settings = settings.model_copy(update={"auto_publish": True})
    v1_summary, v1_refreshed = await orchestrate_run(
        dataset="pudl_test",
        downloader=TestDownloader(v1_resources, session="session"),
        run_settings=settings,
        session="session",
    )
    assert v1_summary.success
    verify_files(test_files["original"], tmp_path / "published")

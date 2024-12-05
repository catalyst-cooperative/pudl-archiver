"""Test fsspec based depositor backend."""

import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import LICENSES

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings


@pytest_asyncio.fixture()
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
def datasource():
    """Create fake datasource for testing."""
    return DataSource(
        name="pudl_test",
        title="Pudl Test",
        description="Test dataset for the sandbox, thanks!",
        path="https://fake.link",
        license_raw=LICENSES["cc-by-4.0"],
        license_pudl=LICENSES["cc-by-4.0"],
    )


@pytest.mark.asyncio
async def test_fsspec_depositor(
    test_files: dict[str, list[dict[str, str]]],
    datasource: DataSource,
    mocker,
    tmp_path,
):
    """Test fsspec depositor backend."""
    settings = RunSettings(
        sandbox=False,
        clobber_unchanged=True,
        auto_publish=False,
        refresh_metadata=False,
        initialize=True,
        depositor="fsspec",
        deposition_path=str(tmp_path),
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

    # Mock out creating datapackage with fake data source
    datasource_mock = mocker.MagicMock(return_value=datasource)
    mocker.patch(
        "pudl_archiver.frictionless.DataSource.from_id",
        new=datasource_mock,
    )

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
        downloader=TestDownloader(v1_resources, session="sesion"),
        run_settings=settings,
        session="sesion",
    )
    assert v1_summary.success
    verify_files(test_files["original"], tmp_path)

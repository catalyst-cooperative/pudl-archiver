"""Test archiver pudl_archiver."""

import os

import pytest

from pudl_archiver import archive_dataset
from pudl_archiver.archivers.validate import RunSummary, ValidationTestResult
from pudl_archiver.utils import RunSettings


@pytest.fixture()
def successful_run():
    """Return a run_summary with a success."""
    return RunSummary(
        dataset_name="success",
        validation_tests=[
            ValidationTestResult(
                name="succesful_test",
                description="succesful test",
                success=True,
            )
        ],
        file_changes=[],
        date="date",
        previous_version_date="date",
        record_url="https://www.catalyst.coop/bogus-record-url",
        datapackage_changed=False,
        successful_partitions={"resource_0": {"part": 0}},
        failed_partitions={},
        run_settings=RunSettings(),
    )


@pytest.fixture()
def failed_run():
    """Return a run_summary with a success."""
    return RunSummary(
        dataset_name="failure",
        validation_tests=[
            ValidationTestResult(
                name="failure_test",
                description="failure test",
                success=False,
            )
        ],
        file_changes=[],
        date="date",
        previous_version_date="date",
        record_url="https://www.catalyst.coop/bogus-record-url",
        datapackage_changed=False,
        successful_partitions={"resource_0": {"part": 0}},
        failed_partitions={"resource_1": {"part": 1}},
        run_settings=RunSettings(),
    )


@pytest.mark.asyncio
async def test_archive_dataset(
    successful_run: RunSummary,
    failed_run: RunSummary,
    mocker,
    tmp_path,
):
    """Test that archive datasets creates run_summary file."""
    mocker.patch.dict(
        "os.environ",
        {
            "ZENODO_SANDBOX_TOKEN_UPLOAD": "bogus",
            "ZENODO_SANDBOX_TOKEN_PUBLISH": "bogus too",
        },
    )
    summary_file = tmp_path / "summary_file"
    with summary_file.open("w") as f:
        f.write("file")
    mocked_json_dump = mocker.patch("pudl_archiver.json.dumps", return_value="{}")
    settings = RunSettings(summary_file=str(summary_file))

    # Set run() return value to success summary and test
    mocked_orchestrator_success = mocker.AsyncMock(
        return_value=(successful_run, "published")
    )
    mocker.patch("pudl_archiver.orchestrate_run", new=mocked_orchestrator_success)
    await archive_dataset("eia860", run_settings=settings)
    mocked_json_dump.assert_called_once_with(successful_run.model_dump(), indent=2)

    # Set run() return value to failure summary and test
    mocked_json_dump.reset_mock()
    mocked_orchestrator_fail = mocker.AsyncMock(return_value=(failed_run, "published"))
    mocker.patch("pudl_archiver.orchestrate_run", new=mocked_orchestrator_fail)
    with pytest.raises(RuntimeError):
        await archive_dataset("eia860", run_settings=settings)
    mocked_json_dump.assert_called_once_with(failed_run.model_dump(), indent=2)


@pytest.mark.asyncio
async def test_summary_written_when_archiver_changes_directory(
    successful_run: RunSummary,
    mocker,
    monkeypatch,
    tmp_path,
):
    """Test that the run summary is written where requested even if the archiver chdirs.

    Archivers ``os.chdir`` into their download directory during a run, so a relative
    summary file path must not be resolved against that directory.
    """
    mocker.patch.dict(
        "os.environ",
        {
            "ZENODO_SANDBOX_TOKEN_UPLOAD": "bogus",
            "ZENODO_SANDBOX_TOKEN_PUBLISH": "bogus too",
        },
    )
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    def run_in_work_dir(**kwargs):
        os.chdir(work_dir)
        return (successful_run, "published")

    mocker.patch(
        "pudl_archiver.orchestrate_run",
        new=mocker.AsyncMock(side_effect=run_in_work_dir),
    )
    settings = RunSettings(summary_file="eia_run_summary.json")
    await archive_dataset("eia860", run_settings=settings)

    assert (tmp_path / "eia_run_summary.json").exists()
    assert not (work_dir / "eia_run_summary.json").exists()

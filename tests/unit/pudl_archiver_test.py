"""Test archiver pudl_archiver."""

import pytest

from pudl_archiver import archive_datasets
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
async def test_archive_datasets(
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
    await archive_datasets(["eia860"], run_settings=settings)
    mocked_json_dump.assert_called_once_with([successful_run.model_dump()], indent=2)

    # Set run() return value to failure summary and test
    mocked_json_dump.reset_mock()
    mocked_orchestrator_fail = mocker.AsyncMock(return_value=(failed_run, "published"))
    mocker.patch("pudl_archiver.orchestrate_run", new=mocked_orchestrator_fail)
    with pytest.raises(RuntimeError):
        await archive_datasets(["eia860"], run_settings=settings)
    mocked_json_dump.assert_called_once_with([failed_run.model_dump()], indent=2)

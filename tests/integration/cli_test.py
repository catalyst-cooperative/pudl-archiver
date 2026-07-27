"""Test CLI and that it properly orchestrates runs."""

import unittest

import aiohttp
import pytest
from click.testing import CliRunner

from pudl_archiver.archivers.eia.eia860 import Eia860Archiver
from pudl_archiver.archivers.ferc.ferc1 import Ferc1Archiver
from pudl_archiver.archivers.validate import RunSummary, ValidationTestResult
from pudl_archiver.cli import pudl_archiver
from pudl_archiver.utils import RunSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,dataset,success,downloader,depositor,expected_depositor_args",
    [
        (
            ["archive", "zenodo", "eia860"],
            "eia860",
            True,
            Eia860Archiver,
            "zenodo",
            {"sandbox": False},
        ),
        (
            ["archive", "zenodo", "eia860"],
            "eia860",
            False,
            Eia860Archiver,
            "zenodo",
            {"sandbox": False},
        ),
        (
            ["archive", "fsspec", "ferc1", "./test_path"],
            "ferc1",
            True,
            Ferc1Archiver,
            "fsspec",
            {"depositor_path": "./test_path"},
        ),
        (
            ["archive", "fsspec", "ferc1", "./test_path"],
            "ferc1",
            False,
            Ferc1Archiver,
            "fsspec",
            {"depositor_path": "./test_path"},
        ),
    ],
)
async def test_cli(
    args, dataset, success, downloader, depositor, expected_depositor_args, mocker
):
    """Test that CLI args are properly handled and runs are orchestrated correctly."""

    def _get_run_results(dataset, **kwargs) -> RunSummary:
        return RunSummary(
            dataset_name="test_dataset",
            validation_tests=[
                ValidationTestResult(
                    name="test_test",
                    description="test of the tests",
                    success=success,
                )
            ],
            file_changes=[],
            date="right_now",
            previous_version_date="before_right_now",
            record_url="https://test.com",
            datapackage_changed=True,
            failed_partitions={},
            successful_partitions={},
            run_settings=RunSettings(),
        ), None

    orchestrate_run_mock = unittest.mock.AsyncMock(side_effect=_get_run_results)
    mocker.patch("pudl_archiver.orchestrate_run", new=orchestrate_run_mock)
    asyncio_run_mock = unittest.mock.MagicMock()
    mocker.patch("pudl_archiver.cli.asyncio.run", new=asyncio_run_mock)

    # Run CLI
    runner = CliRunner()
    if not success:
        with pytest.raises(RuntimeError, match="Archive validation failed:"):
            runner.invoke(pudl_archiver, args)
            (future,) = asyncio_run_mock.call_args.args
            await future
    else:
        runner.invoke(pudl_archiver, args)
        (future,) = asyncio_run_mock.call_args.args
        await future

    _, kwargs = orchestrate_run_mock.await_args
    assert kwargs["dataset"] == dataset
    assert type(kwargs["downloader"]) is downloader
    assert isinstance(kwargs["session"], aiohttp.ClientSession)
    assert isinstance(kwargs["run_settings"], RunSettings)


@pytest.mark.parametrize(
    "failed_partitions,validation_success",
    [
        ({"eia860-2020.zip": {"year": 2020}}, True),
        ({}, False),
    ],
    ids=["failed_partitions", "failed_validation"],
)
def test_publish_run_rejects_unsuccessful_run(
    failed_partitions, validation_success, tmp_path, mocker
):
    """``publish-run`` should refuse to run against a summary with failures.

    ``publish-run`` is only meant to publish the results of a run that fully
    succeeded but wasn't auto-published. If the previous run has any failed
    partitions, or otherwise didn't pass validation, it should raise rather than
    silently publish an incomplete archive -- the user is expected to use
    ``retry-run`` instead.
    """
    summary = RunSummary(
        dataset_name="test_dataset",
        validation_tests=[
            ValidationTestResult(
                name="test_test",
                description="test of the tests",
                success=validation_success,
            )
        ],
        file_changes=[],
        date="right_now",
        previous_version_date="before_right_now",
        record_url="https://test.com",
        datapackage_changed=True,
        failed_partitions=failed_partitions,
        successful_partitions={},
        run_settings=RunSettings(),
    )
    summary_file = tmp_path / "run_summary.json"
    summary_file.write_text(summary.model_dump_json())

    archive_dataset_mock = unittest.mock.AsyncMock()
    mocker.patch("pudl_archiver.cli.archive_dataset", new=archive_dataset_mock)

    runner = CliRunner()
    with pytest.raises(RuntimeError, match="publish-run should not be called"):
        runner.invoke(
            pudl_archiver,
            ["publish-run", str(summary_file)],
            catch_exceptions=False,
        )

    archive_dataset_mock.assert_not_awaited()

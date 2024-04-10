"""Test CLI and that it properly orchestrates runs."""

import unittest

import aiohttp
import pytest
from pudl_archiver.archivers.eia.eia860 import Eia860Archiver
from pudl_archiver.archivers.ferc.ferc1 import Ferc1Archiver
from pudl_archiver.archivers.validate import RunSummary, ValidationTestResult
from pudl_archiver.cli import main
from pudl_archiver.utils import RunSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,dataset_success_map,downloaders",
    [
        (["--datasets", "eia860"], {"eia860": True}, [Eia860Archiver]),
        (["--datasets", "eia860"], {"eia860": False}, [Eia860Archiver]),
        (
            ["--datasets", "eia860", "ferc1"],
            {"eia860": True, "ferc1": True},
            [Eia860Archiver, Ferc1Archiver],
        ),
        (
            ["--datasets", "eia860", "ferc1"],
            {"eia860": True, "ferc1": False},
            [Eia860Archiver, Ferc1Archiver],
        ),
    ],
)
async def test_cli(args, dataset_success_map, downloaders, mocker):
    """Test that CLI args are properly handled and runs are orchestrated correctly."""

    def _get_run_results(dataset, *args) -> RunSummary:
        return RunSummary(
            dataset_name="test_dataset",
            validation_tests=[
                ValidationTestResult(
                    name="test_test",
                    description="test of the tests",
                    success=dataset_success_map[dataset],
                )
            ],
            file_changes=[],
            date="right_now",
            previous_version_date="before_right_now",
            record_url="https://test.com",
        ), None

    mock = unittest.mock.AsyncMock(side_effect=_get_run_results)
    mocker.patch("pudl_archiver.orchestrate_run", new=mock)

    # Run CLI
    if not all(dataset_success_map.values()):
        with pytest.raises(
            RuntimeError, match="Error: archive validation tests failed."
        ):
            await main(args=args)
    else:
        await main(args=args)

    for (dataset, downloader, run_settings, session), _ in mock.await_args_list:
        assert dataset in dataset_success_map
        assert type(downloader) in downloaders
        assert isinstance(session, aiohttp.ClientSession)
        assert isinstance(run_settings, RunSettings)

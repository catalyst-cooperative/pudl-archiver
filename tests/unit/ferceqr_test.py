"""Test how the FERC EQR archiver describes the quarters of data it finds."""

import pandas as pd
import pytest

from pudl_archiver.archivers.classes import VALID_PARTITION_RANGES
from pudl_archiver.archivers.ferc.ferceqr import FercEQRArchiver


def _urls(*skip: str) -> list[str]:
    quarters = pd.period_range("2013q3", "2026q3", freq="Q")
    return [
        f"https://example.com/CSV_{q.year}_Q{q.quarter}.zip"
        for q in quarters
        if f"{q.year}q{q.quarter}" not in skip
    ]


async def _get_resources(mocker, urls):
    archiver = FercEQRArchiver(session=mocker.MagicMock())
    mocker.patch.object(archiver, "get_urls", return_value=urls)
    found = []
    async for resource, partitions in archiver.get_resources():
        resource.close()  # Don't download anything
        found.append(partitions)
    return archiver, found


@pytest.mark.asyncio
async def test_quarters_are_partitioned_by_year_quarter(mocker):
    archiver, found = await _get_resources(mocker, _urls())

    assert found[0] == {"year_quarter": "2013q3"}
    assert found[-1] == {"year_quarter": "2026q3"}
    assert len(found) == 53
    # In a form that the archiver's validation of continuity understands
    assert list(found[0]) == ["year_quarter"]
    assert "year_quarter" in VALID_PARTITION_RANGES
    pd.PeriodIndex([p["year_quarter"] for p in found], freq="Q")
    # The newest quarter is expected to grow, both in a new archive and in one that
    # was partitioned by year and quarter
    assert archiver.ignore_file_size_increase_partitions == [
        {"year_quarter": "2026q3"},
        {"year": 2026, "quarter": 3},
    ]


@pytest.mark.asyncio
async def test_missing_quarter_fails_before_downloading(mocker):
    with pytest.raises(RuntimeError, match=r"Missing quarters.*\['2023q3'\]"):
        await _get_resources(mocker, _urls("2023q3"))


@pytest.mark.asyncio
async def test_first_quarter_missing_fails(mocker):
    with pytest.raises(RuntimeError, match="start in 2013q3"):
        await _get_resources(mocker, _urls("2013q3"))

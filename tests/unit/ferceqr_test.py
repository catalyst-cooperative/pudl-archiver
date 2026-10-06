"""Test how the FERC EQR archiver describes the quarters of data it finds."""

from types import SimpleNamespace

import pandas as pd
import pytest
from playwright.async_api import Error as PlaywrightError

from pudl_archiver.archivers.classes import VALID_PARTITION_RANGES
from pudl_archiver.archivers.ferc.ferceqr import FercEQRArchiver


def _urls(*skip: str) -> list[str]:
    quarters = pd.period_range("2013q3", "2026q3", freq="Q")
    return [
        f"https://example.com/CSV_{q.year}_Q{q.quarter}.zip"
        for q in quarters
        if f"{q.year}q{q.quarter}" not in skip
    ]


def _archived_before(*skip: str):
    """The previous archive, which had every quarter up to 2026q2."""
    names = [
        f"ferceqr-{q.year}q{q.quarter}.zip"
        for q in pd.period_range("2013q3", "2026q2", freq="Q")
        if f"{q.year}q{q.quarter}" not in skip
    ]
    return SimpleNamespace(resources=[SimpleNamespace(name=name) for name in names])


async def _get_resources(mocker, urls, baseline=None):
    archiver = FercEQRArchiver(session=mocker.MagicMock())
    archiver.baseline_datapackage = baseline
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
    # The newest quarter is expected to grow
    assert archiver.ignore_file_size_increase_partitions == [{"year_quarter": "2026q3"}]


@pytest.mark.asyncio
async def test_missing_quarter_fails_before_downloading(mocker):
    with pytest.raises(RuntimeError, match=r"Missing quarters.*\['2023q3'\]"):
        await _get_resources(mocker, _urls("2023q3"))


@pytest.mark.asyncio
async def test_first_quarter_missing_fails(mocker):
    with pytest.raises(RuntimeError, match="start in 2013q3"):
        await _get_resources(mocker, _urls("2013q3"))


@pytest.mark.asyncio
async def test_loading_the_page_is_retried(mocker):
    mocker.patch("pudl_archiver.utils.asyncio.sleep")  # Don't wait between attempts
    archiver = FercEQRArchiver(session=mocker.MagicMock())
    get_urls = mocker.patch.object(
        archiver,
        "get_urls",
        side_effect=[PlaywrightError("Page.goto: Operation was cancelled"), _urls()],
    )

    async for resource, _ in archiver.get_resources():
        resource.close()

    assert get_urls.call_count == 2


@pytest.mark.asyncio
async def test_loading_the_page_gives_up_eventually(mocker):
    mocker.patch("pudl_archiver.utils.asyncio.sleep")
    archiver = FercEQRArchiver(session=mocker.MagicMock())
    get_urls = mocker.patch.object(
        archiver, "get_urls", side_effect=PlaywrightError("Page.goto: Timeout")
    )

    with pytest.raises(PlaywrightError, match="Timeout"):
        await anext(archiver.get_resources())

    assert get_urls.call_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("vanished", ["2013q3", "2023q3", "2026q2"])
async def test_quarters_that_vanished_from_the_website_are_named(mocker, vanished):
    # Only the latest, which no gap shows, or the first, can also go
    with pytest.raises(RuntimeError, match=rf"have vanished.*\['{vanished}'\]"):
        await _get_resources(
            mocker, _urls(vanished, "2026q3"), baseline=_archived_before()
        )


@pytest.mark.asyncio
async def test_new_quarters_are_fine(mocker):
    # 2026q3 is on the website, and wasn't in the previous archive
    _, found = await _get_resources(mocker, _urls(), baseline=_archived_before())

    assert {"year_quarter": "2026q3"} in found


def test_vanished_quarters_are_not_checked_when_missing_files_are_allowed(mocker):
    archiver = FercEQRArchiver(session=mocker.MagicMock())
    archiver.baseline_datapackage = _archived_before()
    archiver.fail_on_missing_files = False

    archiver._check_no_quarters_have_vanished({"2013q3"})


@pytest.mark.asyncio
async def test_finding_no_quarters_fails(mocker):
    with pytest.raises(RuntimeError, match="no quarters"):
        await _get_resources(mocker, [])

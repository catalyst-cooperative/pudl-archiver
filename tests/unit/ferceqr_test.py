"""Test how the FERC EQR archiver describes the quarters of data it finds."""

from types import SimpleNamespace

import pandas as pd
import pytest
from playwright.async_api import Error as PlaywrightError

from pudl_archiver.archivers.classes import VALID_PARTITION_RANGES, PriorState
from pudl_archiver.archivers.ferc.ferceqr import FercEQRArchiver

# Arbitrary quarters, deliberately not the real first and last ones, so that the
# tests are about how quarters are handled and not about which ones exist.
FIRST = "2015q2"
MIDDLE = "2016q3"
LAST_ARCHIVED = "2017q4"  # The last quarter of the previous archive
LAST_ONLINE = "2018q1"  # Newer than the previous archive
N_ONLINE = 12  # Quarters from FIRST through LAST_ONLINE


@pytest.fixture(autouse=True)
def _first_quarter(monkeypatch):
    """Make the archiver expect the data to start in the fake ``FIRST`` quarter."""
    monkeypatch.setattr(
        "pudl_archiver.archivers.ferc.ferceqr.FIRST_YEAR_QUARTER", FIRST
    )


def _quarter_labels(last: str, skip: tuple[str, ...]) -> list[str]:
    """List quarter labels like ``2015q2`` from ``FIRST`` through ``last``."""
    return [
        label
        for q in pd.period_range(FIRST, last, freq="Q")
        if (label := f"{q.year}q{q.quarter}") not in skip
    ]


def _urls(*skip: str) -> list[str]:
    """Make the download links on the website, from ``FIRST`` through ``LAST_ONLINE``."""
    return [
        f"https://example.com/CSV_{label[:4]}_Q{label[-1]}.zip"
        for label in _quarter_labels(LAST_ONLINE, skip)
    ]


def _archived_before(*skip: str) -> SimpleNamespace:
    """Make a stand-in for the previous archive's datapackage.

    The previous archive had every quarter through ``LAST_ARCHIVED``.
    """
    names = [f"ferceqr-{label}.zip" for label in _quarter_labels(LAST_ARCHIVED, skip)]
    return SimpleNamespace(resources=[SimpleNamespace(name=name) for name in names])


def _archiver(mocker, baseline=None) -> FercEQRArchiver:
    """Make an archiver that has no session and a given previous archive."""
    return FercEQRArchiver(
        session=mocker.MagicMock(),
        prior_state=PriorState(baseline_datapackage=baseline),
    )


async def _get_resources(mocker, urls, baseline=None):
    """Run ``get_resources`` on a fixed list of links, without downloading anything."""
    archiver = _archiver(mocker, baseline)
    mocker.patch.object(archiver, "get_urls", return_value=urls)
    found = []
    async for resource, partitions in archiver.get_resources():
        resource.close()  # Don't download anything
        found.append(partitions)
    return archiver, found


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "baseline", [None, _archived_before()], ids=["no-baseline", "baseline"]
)
async def test_quarters_are_partitioned_by_year_quarter(mocker, baseline):
    """Check each quarter is yielded as a ``year_quarter`` partition.

    The newest quarter, ``LAST_ONLINE``, is only on the website and not in the previous
    archive, so the baseline case also checks that new quarters are not flagged.
    """
    archiver, found = await _get_resources(mocker, _urls(), baseline)

    assert found[0] == {"year_quarter": FIRST}
    assert found[-1] == {"year_quarter": LAST_ONLINE}
    assert len(found) == N_ONLINE
    # The key is one that the archiver's validation of continuity understands
    assert "year_quarter" in VALID_PARTITION_RANGES
    # The newest quarter is expected to grow
    assert archiver.ignore_file_size_increase_partitions == [
        {"year_quarter": LAST_ONLINE}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "urls,message",
    [
        (_urls(MIDDLE), rf"Missing quarters.*\['{MIDDLE}'\]"),
        (_urls(FIRST), f"start in {FIRST}"),
        ([], "no quarters"),
    ],
    ids=["gap", "first-quarter-missing", "empty"],
)
async def test_bad_quarters_fail_before_downloading(mocker, urls, message):
    """Check that a gap, a late start or no quarters at all fails the run early.

    Since ``_get_resources`` closes every resource without downloading it, an error
    here can only have come from the checks made before any download starts.
    """
    with pytest.raises(RuntimeError, match=message):
        await _get_resources(mocker, urls)


@pytest.mark.asyncio
async def test_loading_the_page_is_retried_then_gives_up(mocker):
    """Check that failing to load the page is retried, and eventually raised.

    How ``retry_async`` backs off is tested in ``utils_test``. This only checks that
    the archiver retries Playwright errors, and for how many attempts.
    """
    mocker.patch("pudl_archiver.utils.asyncio.sleep")  # Don't wait between attempts
    archiver = _archiver(mocker)
    get_urls = mocker.patch.object(
        archiver, "get_urls", side_effect=PlaywrightError("Page.goto: Timeout")
    )

    with pytest.raises(PlaywrightError, match="Timeout"):
        await anext(archiver.get_resources())

    assert get_urls.call_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("vanished", [FIRST, MIDDLE, LAST_ARCHIVED])
async def test_quarters_that_vanished_from_the_website_are_named(mocker, vanished):
    """Check that quarters in the previous archive, but not online, are reported.

    The first quarter, a quarter in the middle and the last quarter before the newest
    one are each removed. The newest quarter, ``LAST_ONLINE``, is also removed, so that
    the last previous quarter vanishing doesn't leave a gap to be noticed.
    """
    with pytest.raises(RuntimeError, match=rf"have vanished.*\['{vanished}'\]"):
        await _get_resources(
            mocker, _urls(vanished, LAST_ONLINE), baseline=_archived_before()
        )


def test_vanished_quarters_are_not_checked_when_missing_files_are_allowed(mocker):
    """Check that the vanished-quarters check is skipped if missing files are OK."""
    archiver = _archiver(mocker, _archived_before())
    archiver.fail_on_missing_files = False

    archiver._check_no_quarters_have_vanished({FIRST})

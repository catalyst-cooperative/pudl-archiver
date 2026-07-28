"""Tests for the EPA CEMS and MATS archivers.

These mock the underlying ``aiohttp`` session (rather than ``EpaCemsArchiver``/
``EpaMatsArchiver`` methods directly), so they exercise the real
``self.get_json`` code path -- including JSON decoding -- the same way it
would run against the live API. Fixture bodies use plain double-quoted JSON,
matching what the real EPA bulk-files API actually returns (confirmed against
a live request; there is no single-quote/non-standard-JSON quirk to work
around today).
"""

import json

import pytest

from pudl_archiver.archivers.epa.epacems import EpaCemsArchiver
from pudl_archiver.archivers.epa.epamats import EpaMatsArchiver


def _bulk_file(
    filename: str,
    year: int,
    quarter: int,
    data_type: str,
    data_sub_type: str = "Hourly",
    *,
    complete: bool = True,
) -> dict:
    """Build a single bulk-file entry as the EPA API would represent it.

    Field names use the API's camelCase aliases, matching what the archivers
    actually receive over the wire.
    """
    entry = {
        "filename": filename,
        "s3Path": f"path/to/{filename}",
        "bytes": 1234,
        "megaBytes": 1.234,
        "gigaBytes": 0.001234,
        "lastUpdated": "2024-01-01T00:00:00",
        "metadata": {
            "year": year,
            "quarter": quarter,
            "dataType": data_type,
            "dataSubType": data_sub_type,
        },
    }
    if not complete:
        # Drop a required field to trigger pydantic validation failure, which
        # both archivers are expected to silently skip (e.g. the real API's
        # per-state-year aggregate files, which have no "quarter" field).
        del entry["bytes"]
    return entry


def _mock_session_get(mocker, response_bytes: bytes):
    """Build a mocked aiohttp session whose .get() resolves to response_bytes.

    This mirrors how aiohttp.ClientSession.get is actually awaited inside
    AbstractDatasetArchiver.get_json, so the real decode/json.loads logic in
    get_json runs during the test.
    """
    fake_response = mocker.MagicMock()
    fake_response.read = mocker.AsyncMock(return_value=response_bytes)
    mock_session = mocker.AsyncMock()
    mock_session.get = mocker.AsyncMock(return_value=fake_response)
    return mock_session


@pytest.mark.parametrize(
    "archiver_cls,valid_data_type",
    [
        (EpaCemsArchiver, "Emissions"),
        (EpaMatsArchiver, "Mercury and Air Toxics Emissions (MATS)"),
    ],
)
@pytest.mark.asyncio
async def test_get_resources_parses_response_and_filters(
    mocker, archiver_cls, valid_data_type
):
    """get_resources should decode the API response and filter files.

    This locks in behavior end to end through the real self.get_json path:
    valid files should be grouped by year, and files with the wrong data
    type/sub-type/quarter, or that fail to validate (e.g. missing the
    "quarter" field the way the real per-state-year aggregate files do),
    should be silently dropped.
    """
    files = [
        _bulk_file("valid-2020-q1.csv", 2020, 1, valid_data_type),
        _bulk_file("valid-2020-q2.csv", 2020, 2, valid_data_type),
        _bulk_file("valid-2021-q3.csv", 2021, 3, valid_data_type),
        # Wrong data type - should be filtered out.
        _bulk_file("wrong-type.csv", 2020, 3, "Some Other Data"),
        # Wrong data sub type - should be filtered out.
        _bulk_file(
            "wrong-subtype.csv", 2020, 3, valid_data_type, data_sub_type="Daily"
        ),
        # Invalid quarter - should be filtered out.
        _bulk_file("bad-quarter.csv", 2020, 5, valid_data_type),
        # Missing required field - should fail pydantic validation and be
        # silently dropped by __filter_for_complete_metadata.
        _bulk_file("incomplete.csv", 2020, 4, valid_data_type, complete=False),
    ]
    response_bytes = json.dumps({"items": files}).encode("utf8")
    mock_session = _mock_session_get(mocker, response_bytes)
    archiver = archiver_cls(mock_session)

    get_year_resource = mocker.patch.object(
        archiver, "get_year_resource", mocker.AsyncMock(return_value="resource")
    )

    resources = [res async for res in archiver.get_resources()]
    # Await the mocked coroutines so nothing is left unawaited.
    for res in resources:
        await res

    assert get_year_resource.call_count == 2

    calls_by_year = {
        call.args[0]: call.args[1] for call in get_year_resource.call_args_list
    }
    assert set(calls_by_year) == {2020, 2021}

    year_2020_filenames = {f.filename for f in calls_by_year[2020]}
    assert year_2020_filenames == {"valid-2020-q1.csv", "valid-2020-q2.csv"}

    year_2021_filenames = {f.filename for f in calls_by_year[2021]}
    assert year_2021_filenames == {"valid-2021-q3.csv"}


@pytest.mark.parametrize(
    "archiver_cls,valid_data_type",
    [
        (EpaCemsArchiver, "Emissions"),
        (EpaMatsArchiver, "Mercury and Air Toxics Emissions (MATS)"),
    ],
)
@pytest.mark.asyncio
async def test_get_resources_respects_only_years(mocker, archiver_cls, valid_data_type):
    """get_resources should only yield years in only_years, when set."""
    files = [
        _bulk_file("2019.csv", 2019, 1, valid_data_type),
        _bulk_file("2020.csv", 2020, 1, valid_data_type),
        _bulk_file("2021.csv", 2021, 1, valid_data_type),
    ]
    response_bytes = json.dumps({"items": files}).encode("utf8")
    mock_session = _mock_session_get(mocker, response_bytes)
    archiver = archiver_cls(mock_session, only_years=[2020])

    get_year_resource = mocker.patch.object(
        archiver, "get_year_resource", mocker.AsyncMock(return_value="resource")
    )

    resources = [res async for res in archiver.get_resources()]
    for res in resources:
        await res

    assert get_year_resource.call_count == 1
    (call,) = get_year_resource.call_args_list
    assert call.args[0] == 2020


@pytest.mark.parametrize(
    "archiver_cls",
    [EpaCemsArchiver, EpaMatsArchiver],
)
@pytest.mark.asyncio
async def test_get_resources_raises_on_api_error(mocker, archiver_cls):
    """get_resources should raise clearly if the bulk-files API errors out.

    The EPA API returns valid (if unhelpful) JSON on failure, e.g. a 403 for
    an invalid API key returns {"error": {"code": ..., "message": ...}} with
    no "items" key, so get_json alone won't raise -- get_resources needs to
    check for "items" explicitly to fail with a clear message instead of a
    bare KeyError.
    """
    error_body = json.dumps(
        {"error": {"code": "API_KEY_INVALID", "message": "invalid key"}}
    ).encode("utf8")
    mock_session = _mock_session_get(mocker, error_body)
    archiver = archiver_cls(mock_session)

    with pytest.raises(AssertionError, match="did not succeed"):
        [res async for res in archiver.get_resources()]


@pytest.mark.parametrize(
    "archiver_cls",
    [EpaCemsArchiver, EpaMatsArchiver],
)
@pytest.mark.asyncio
async def test_get_resources_raises_on_invalid_json(mocker, archiver_cls):
    """get_resources should raise clearly if the response isn't valid JSON.

    This is a regression guard: the archivers used to defensively rewrite
    single quotes to double quotes before parsing, on the assumption the API
    might return non-standard JSON. A live request confirmed the API returns
    proper double-quoted JSON today, so that rewrite was dropped. If the API
    ever reverts, this should fail loudly via get_json's own JSON validation
    rather than silently mangling data.
    """
    mock_session = _mock_session_get(mocker, b"{'not': 'valid json'}")
    archiver = archiver_cls(mock_session)

    with pytest.raises(AssertionError, match="Invalid JSON string"):
        [res async for res in archiver.get_resources()]

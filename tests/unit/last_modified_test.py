"""Test using server metadata to predict which files have changed."""

import datetime

import pytest

from pudl_archiver.archivers.classes import parse_http_date
from pudl_archiver.archivers.validate import RunSummary, create_last_modified_checks
from pudl_archiver.frictionless import Resource, SourceMetadata

UTC = datetime.UTC


def _dt(day: int) -> datetime.datetime:
    return datetime.datetime(2026, 10, day, tzinfo=UTC)


def _resource(hash_: str, size: int = 10, source_metadata=None) -> Resource:
    return Resource(
        name="a.zip",
        path="gs://bucket/a.zip",
        title="a.zip",
        parts={},
        mediatype="application/zip",
        format=".zip",
        bytes=size,
        hash=hash_,
        source_metadata=source_metadata,
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            "Sat, 03 Oct 2026 07:19:42 GMT",
            datetime.datetime(2026, 10, 3, 7, 19, 42, tzinfo=UTC),
        ),
        (None, None),
        ("yesterday-ish", None),
    ],
)
def test_parse_http_date(value, expected):
    assert parse_http_date(value) == expected


def test_source_metadata_is_only_serialized_when_recorded():
    assert "source_metadata" not in _resource("x").model_dump(by_alias=True)
    metadata = SourceMetadata(last_modified=_dt(3), etag='"abc"', content_length=5)
    dumped = _resource("x", source_metadata=metadata).model_dump(by_alias=True)
    assert Resource.model_validate(dumped).source_metadata == metadata


def test_run_summary_from_before_last_modified_checks_still_loads():
    summary = RunSummary.model_validate(
        {
            "dataset_name": "x",
            "validation_tests": [],
            "file_changes": [],
            "date": "d",
            "previous_version_date": "d",
            "record_url": "gs://x",
            "datapackage_changed": False,
            "failed_partitions": {},
            "successful_partitions": {},
            "run_settings": {},
        }
    )
    assert summary.last_modified_checks == []


STORED = SourceMetadata(last_modified=_dt(1), etag='"a"', content_length=10)


@pytest.mark.parametrize(
    "observed,baseline,new_hash,expected",
    # expected: reference, last_modified_changed, size_differs, predicted_changed
    [
        # Previously uploaded on day 2, and no metadata was stored then
        (SourceMetadata(last_modified=_dt(3)), _resource("old"), "new", ("previous_upload_time", True, None, True)),
        (SourceMetadata(last_modified=_dt(1)), _resource("old"), "old", ("previous_upload_time", False, None, False)),
        # A changed file that Last-Modified missed: a false negative
        (SourceMetadata(last_modified=_dt(1)), _resource("old"), "new", ("previous_upload_time", False, None, False)),
        # The size proves a change even though Last-Modified says there wasn't
        (SourceMetadata(last_modified=_dt(1), content_length=11), _resource("old"), "new", ("previous_upload_time", False, True, True)),
        (SourceMetadata(last_modified=_dt(1), content_length=10), _resource("old"), "old", ("previous_upload_time", False, False, False)),
        # Nothing to predict from
        (SourceMetadata(), _resource("old"), "new", ("none", None, None, None)),
        (SourceMetadata(last_modified=_dt(3)), None, "new", ("new_file", True, None, True)),
        # Metadata was stored last time, so compare directly with it
        (STORED, _resource("old", source_metadata=STORED), "old", ("stored_metadata", False, False, False)),
        # An older file replacing a newer one is still a change
        (SourceMetadata(last_modified=_dt(1), etag='"b"'), _resource("old", source_metadata=STORED), "new", ("stored_metadata", True, None, True)),
    ],
)  # fmt: skip
def test_last_modified_checks(observed, baseline, new_hash, expected):
    [check] = create_last_modified_checks(
        {"a.zip": observed},
        {"a.zip": baseline} if baseline else {},
        {"a.zip": _resource(new_hash)},
        previous_upload_times={"a.zip": _dt(2)},
    )

    assert (
        check.reference,
        check.last_modified_changed,
        check.size_differs,
        check.predicted_changed,
    ) == expected
    assert check.actually_changed == (baseline is None or new_hash != baseline.hash_)
    assert check.prediction_correct == (
        None
        if check.predicted_changed is None
        else check.predicted_changed == check.actually_changed
    )

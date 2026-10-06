"""Test tracking of server metadata to predict which files have changed."""

import datetime

import pytest

from pudl_archiver.archivers.classes import parse_http_date
from pudl_archiver.archivers.validate import RunSummary, create_last_modified_checks
from pudl_archiver.frictionless import HttpFileMetadata, Resource

UTC = datetime.UTC


def _dt(day: int, hour: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 10, day, hour, tzinfo=UTC)


def _resource(
    name: str,
    hash_: str,
    source_metadata: HttpFileMetadata | None = None,
    size: int = 10,
) -> Resource:
    return Resource(
        name=name,
        path=f"gs://bucket/{name}",
        title=name,
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
        ("", None),
        ("yesterday-ish", None),
    ],
)
def test_parse_http_date(value, expected):
    assert parse_http_date(value) == expected


def test_resource_serialization_omits_missing_source_metadata():
    assert "source_metadata" not in _resource("a.zip", "x").model_dump(by_alias=True)
    meta = HttpFileMetadata(last_modified=_dt(3), etag='"abc"', content_length=5)
    dumped = _resource("a.zip", "x", meta).model_dump(by_alias=True)
    assert dumped["source_metadata"]["etag"] == '"abc"'
    assert Resource.model_validate(dumped).source_metadata == meta


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


STORED = HttpFileMetadata(last_modified=_dt(1), etag='"a"', content_length=10)


@pytest.mark.parametrize(
    "observed,baseline,new_hash,etag_is_content_hash,expected",
    # expected: reference, predicted_changed
    [
        # Previously uploaded on day 2, and no metadata was stored then
        (HttpFileMetadata(last_modified=_dt(3)), _resource("a.zip", "old"), "new", False, ("previous_upload_time", True)),
        (HttpFileMetadata(last_modified=_dt(1)), _resource("a.zip", "old"), "old", False, ("previous_upload_time", False)),
        # A changed file that Last-Modified missed: a false negative
        (HttpFileMetadata(last_modified=_dt(1)), _resource("a.zip", "old"), "new", False, ("previous_upload_time", False)),
        # A different size is a change, whatever Last-Modified says
        (HttpFileMetadata(last_modified=_dt(1), content_length=11), _resource("a.zip", "old"), "new", False, ("previous_upload_time", True)),
        (HttpFileMetadata(last_modified=_dt(1), content_length=10), _resource("a.zip", "old"), "old", False, ("previous_upload_time", False)),
        # Nothing to predict from
        (HttpFileMetadata(), _resource("a.zip", "old"), "new", False, ("none", None)),
        (HttpFileMetadata(last_modified=_dt(3)), None, "new", False, ("new_file", True)),
        # Metadata was stored last time, so compare directly with it
        (STORED, _resource("a.zip", "old", source_metadata=STORED), "old", False, ("stored_metadata", False)),
        # An older file replacing a newer one is still a change
        (HttpFileMetadata(
            last_modified=datetime.datetime(2026, 9, 30, tzinfo=UTC), content_length=10
        ), _resource("a.zip", "old", source_metadata=STORED), "new", False, ("stored_metadata", True)),
        # A different ETag is only a change where ETags are hashes of the contents
        (HttpFileMetadata(last_modified=_dt(1), etag='"b"', content_length=10), _resource("a.zip", "old", source_metadata=STORED), "new", False, ("stored_metadata", False)),
        (HttpFileMetadata(last_modified=_dt(1), etag='"b"', content_length=10), _resource("a.zip", "old", source_metadata=STORED), "new", True, ("stored_metadata", True)),
    ],
)  # fmt: skip
def test_last_modified_checks(
    observed, baseline, new_hash, etag_is_content_hash, expected
):
    [check] = create_last_modified_checks(
        {"a.zip": observed},
        {"a.zip": baseline} if baseline else {},
        {"a.zip": _resource("a.zip", new_hash)},
        previous_upload_times={"a.zip": _dt(2)},
        etag_is_content_hash=etag_is_content_hash,
    )

    assert (check.reference, check.predicted_changed) == expected
    assert check.actually_changed == (baseline is None or new_hash != baseline.hash_)
    assert check.prediction_correct == (
        None
        if check.predicted_changed is None
        else check.predicted_changed == check.actually_changed
    )

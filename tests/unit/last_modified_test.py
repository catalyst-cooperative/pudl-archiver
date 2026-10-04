"""Test tracking of server metadata to predict which files have changed."""

import datetime

import pytest
from upath import UPath

from pudl_archiver.archivers.classes import parse_http_date
from pudl_archiver.archivers.validate import (
    RunSummary,
    create_last_modified_checks,
)
from pudl_archiver.depositors.fsspec import _creation_time
from pudl_archiver.frictionless import Resource, SourceMetadata

UTC = datetime.UTC


def _dt(day: int, hour: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 10, day, hour, tzinfo=UTC)


def _resource(
    name: str,
    hash_: str,
    source_metadata: SourceMetadata | None = None,
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
    meta = SourceMetadata(last_modified=_dt(3), etag='"abc"', content_length=5)
    dumped = _resource("a.zip", "x", meta).model_dump(by_alias=True)
    assert dumped["source_metadata"]["etag"] == '"abc"'
    assert Resource.model_validate(dumped).source_metadata == meta


def test_checks_against_previous_upload_time():
    observed = {
        "newer.zip": SourceMetadata(last_modified=_dt(3)),
        "older.zip": SourceMetadata(last_modified=_dt(1)),
        "false_negative.zip": SourceMetadata(last_modified=_dt(1)),
        "no_header.zip": SourceMetadata(),
        "brand_new.zip": SourceMetadata(last_modified=_dt(3)),
    }
    baseline = {
        n: _resource(n, "old")
        for n in ("newer.zip", "older.zip", "false_negative.zip", "no_header.zip")
    }
    new = {
        "newer.zip": _resource("newer.zip", "new"),
        "older.zip": _resource("older.zip", "old"),
        "false_negative.zip": _resource("false_negative.zip", "new"),
        "no_header.zip": _resource("no_header.zip", "new"),
        "brand_new.zip": _resource("brand_new.zip", "new"),
    }
    uploads = {n: _dt(2) for n in baseline}

    checks = {
        c.name: c for c in create_last_modified_checks(observed, baseline, new, uploads)
    }

    assert checks["newer.zip"].predicted_changed is True
    assert checks["newer.zip"].prediction_correct is True
    assert checks["older.zip"].predicted_changed is False
    assert checks["older.zip"].prediction_correct is True
    assert checks["false_negative.zip"].predicted_changed is False
    assert checks["false_negative.zip"].prediction_correct is False
    assert checks["no_header.zip"].predicted_changed is None
    assert checks["no_header.zip"].prediction_correct is None
    assert checks["brand_new.zip"].reference == "new_file"
    assert checks["brand_new.zip"].prediction_correct is True


def test_size_overrides_last_modified():
    observed = {
        "grew.zip": SourceMetadata(last_modified=_dt(1), content_length=11),
        "same_size.zip": SourceMetadata(last_modified=_dt(1), content_length=10),
    }
    baseline = {n: _resource(n, "old") for n in observed}  # 10 bytes
    new = {n: _resource(n, "new") for n in observed}
    uploads = {n: _dt(2) for n in observed}

    checks = {
        c.name: c for c in create_last_modified_checks(observed, baseline, new, uploads)
    }

    # Last-Modified says unchanged, but the size proves otherwise
    assert checks["grew.zip"].last_modified_changed is False
    assert checks["grew.zip"].size_differs is True
    assert checks["grew.zip"].predicted_changed is True
    assert checks["grew.zip"].prediction_correct is True
    # Same size doesn't prove anything, so Last-Modified decides (here, wrongly)
    assert checks["same_size.zip"].size_differs is False
    assert checks["same_size.zip"].predicted_changed is False
    assert checks["same_size.zip"].prediction_correct is False


def test_checks_against_stored_metadata():
    stored = SourceMetadata(last_modified=_dt(1), etag='"a"', content_length=5)
    observed = {
        "same.zip": stored.model_copy(),
        # An older file replacing a newer one still counts as changed
        "replaced.zip": SourceMetadata(
            last_modified=_dt(1), etag='"b"', content_length=5
        ),
    }
    baseline = {
        "same.zip": _resource("same.zip", "h", stored, size=5),
        "replaced.zip": _resource("replaced.zip", "h", stored, size=5),
    }
    new = {
        "same.zip": _resource("same.zip", "h"),
        "replaced.zip": _resource("replaced.zip", "h2"),
    }

    checks = {
        c.name: c for c in create_last_modified_checks(observed, baseline, new, {})
    }

    assert checks["same.zip"].reference == "stored_metadata"
    assert checks["same.zip"].predicted_changed is False
    assert checks["same.zip"].actually_changed is False
    assert checks["replaced.zip"].predicted_changed is True
    assert checks["replaced.zip"].prediction_correct is True


def test_old_run_summary_without_checks_loads(tmp_path):
    summary = {
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
    assert RunSummary.model_validate(summary).last_modified_checks == []


def test_creation_time_local(tmp_path):
    path = tmp_path / "file.zip"
    path.write_bytes(b"data")
    created = _creation_time(UPath(path))
    assert created is not None
    assert abs(created - datetime.datetime.now(tz=UTC)) < datetime.timedelta(minutes=5)

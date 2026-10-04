"""Test tracking of server metadata to predict which files have changed."""

import datetime

import pytest

from pudl_archiver.archivers.classes import parse_http_date
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

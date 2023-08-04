"""Test archive validate module."""
import pytest

from pudl_archiver.archivers import validate
from pudl_archiver.frictionless import Resource


@pytest.mark.parametrize(
    "baseline_partitions,new_partitions,diffs",
    [
        (
            {"part0": "val0", "part1": "val1"},
            {"part0": "val0", "part1": "val1"},
            [],
        ),
        (
            {"part0": "val0", "part1": "val1"},
            {"part0": "val0", "part1": "val1_changed"},
            [
                validate.PartitionDiff(
                    key="part1",
                    value="val1_changed",
                    previous_value="val1",
                    diff_type="UPDATE",
                )
            ],
        ),
        (
            {"part0": "val0", "part1": "val1_deleted"},
            {"part0": "val0"},
            [
                validate.PartitionDiff(
                    key="part1",
                    previous_value="val1_deleted",
                    diff_type="DELETE",
                )
            ],
        ),
        (
            {"part0": "val0"},
            {"part0": "val0", "part1": "val1_created"},
            [
                validate.PartitionDiff(
                    key="part1",
                    value="val1_created",
                    diff_type="CREATE",
                )
            ],
        ),
    ],
)
def test_process_partition_diffs(baseline_partitions, new_partitions, diffs):
    """Test partition diffs."""
    test_diffs = validate._process_partition_diffs(baseline_partitions, new_partitions)
    assert test_diffs == diffs


def _fake_resource(num=0, **kwargs):
    params = {
        "name": f"resource{num}",
        "path": "https://www.fake.link",
        "remote_url": "https://www.fake.link",
        "title": f"Resource {num}",
        "parts": {},
        "mediatype": "zip",
        "format": "format",
        "bytes": 10,
        "hash": f"hash{num}",
    } | kwargs
    return Resource(**params)


@pytest.mark.parametrize(
    "baseline_resources,new_resources,diffs",
    [
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=40, hash="hash1_changed"),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="UPDATE",
                    size_diff=20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="DELETE",
                    size_diff=-20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="CREATE",
                    size_diff=20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [],
        ),
    ],
)
def test_create_run_summary(baseline_resources, new_resources, diffs, mocker):
    """Test resource diffs."""
    baseline = mocker.MagicMock(
        resources=baseline_resources, version="0.0.1", created="2023-01-01"
    )
    new = mocker.MagicMock(
        resources=new_resources, version="0.0.2", created="2023-01-02"
    )
    summary = validate.RunSummary.create_summary(
        name="test package",
        baseline_datapackage=baseline,
        new_datapackage=new,
        validation_tests=[],
    )
    test_diffs = summary.file_changes
    assert test_diffs == diffs

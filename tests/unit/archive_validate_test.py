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


@pytest.mark.parametrize(
    "baseline_resources,new_resources,diffs",
    [
        (
            [
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=20,
                    hash="hash1",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=40,
                    hash="hash1_changed",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
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
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=20,
                    hash="hash1",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
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
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=20,
                    hash="hash1_created",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
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
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=20,
                    hash="hash1",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
            ],
            [
                Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=10,
                    hash="hash0",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
                Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    bytes=20,
                    hash="hash1",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                ),
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

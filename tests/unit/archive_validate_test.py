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
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=20,
                    hash="hash1",
                ),
            },
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=40,
                    hash="hash1_changed",
                ),
            },
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="UPDATE",
                    size_diff=20,
                )
            ],
        ),
        (
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=20,
                    hash="hash1",
                ),
            },
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
            },
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="DELETE",
                    size_diff=-20,
                )
            ],
        ),
        (
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
            },
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=20,
                    hash="hash1_created",
                ),
            },
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="CREATE",
                    size_diff=20,
                )
            ],
        ),
        (
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=20,
                    hash="hash1",
                ),
            },
            {
                "resource0": Resource(
                    name="resource0",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 0",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=10,
                    hash="hash0",
                ),
                "resource1": Resource(
                    name="resource1",
                    path="https://www.fake.link",
                    remote_url="https://www.fake.link",
                    title="Resource 1",
                    parts={},
                    mediatype="zip",
                    format="format",
                    bytes=20,
                    hash="hash1",
                ),
            },
            [],
        ),
    ],
)
def test_process_resource_diffs(baseline_resources, new_resources, diffs):
    """Test resource diffs."""
    test_diffs = validate._process_resource_diffs(baseline_resources, new_resources)
    assert test_diffs == diffs

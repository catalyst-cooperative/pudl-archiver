"""Defines models used for validating/summarizing an archiver run."""
import zipfile
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from pudl_archiver.frictionless import DataPackage, Resource


class ValidationTestResult(BaseModel):
    """Class containing results of a validation test, and metdata about the test."""

    name: str
    description: str
    ignore_failure: bool = False
    resource_name: str | None = None  # If test is specific to a single resource
    success: bool
    note: str | None = None  # Optional note to provide details like why test failed


class PartitionDiff(BaseModel):
    """Model summarizing changes in partitions."""

    key: Any
    value: str | None = None
    previous_value: str | None = None
    diff_type: Literal["CREATE", "UPDATE", "DELETE"]


class FileValidation(BaseModel):
    """Check that file is valid based on datatype and that it's not empty."""

    valid_type: bool
    not_empty: bool

    @classmethod
    def from_path(cls, path: Path):
        """Validate file type and check that file is not empty."""
        valid_type = True

        # xlsx file should be a zipfile under the hood
        if path.suffix == ".zip" or path.suffix == ".xlsx":
            valid_type = zipfile.is_zipfile(path)

        not_empty = path.stat().st_size > 0
        return cls(valid_type=valid_type, not_empty=not_empty)


class FileDiff(BaseModel):
    """Model summarizing changes to a single file in a deposition."""

    name: str
    diff_type: Literal["CREATE", "UPDATE", "DELETE"]
    size_diff: int
    partition_changes: list[PartitionDiff] = []


class Unchanged(BaseModel):
    """Alternative model to ``RunSummary`` returned when no changes are detected."""

    dataset_name: str
    reason: str = "No changes detected."


class RunSummary(BaseModel):
    """Model summarizing results of an archiver run that can be easily output as JSON."""

    dataset_name: str
    validation_tests: list[ValidationTestResult]
    file_changes: list[FileDiff]
    version: str = ""
    pervious_version: str = ""
    date: str
    previous_version_date: str

    @property
    def success(self) -> bool:
        """Return True if all tests not marked as ``ignore_failure`` passed."""
        test_results = [
            (test.success or test.ignore_failure) for test in self.validation_tests
        ]
        return all(test_results)

    @classmethod
    def create_summary(
        cls,
        name: str,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        validation_tests: list[ValidationTestResult],
    ) -> "RunSummary":
        """Create a summary of archive changes from two DataPackage descriptors."""
        baseline_resources = {}
        if baseline_datapackage is not None:
            baseline_resources = {
                resource.name: resource for resource in baseline_datapackage.resources
            }
        new_resources = {
            resource.name: resource for resource in new_datapackage.resources
        }

        file_changes = _process_resource_diffs(baseline_resources, new_resources)

        previous_version = ""
        previous_version_date = ""
        if baseline_datapackage:
            previous_version = baseline_datapackage.version
            previous_version_date = baseline_datapackage.created

        return cls(
            dataset_name=name,
            validation_tests=validation_tests,
            file_changes=file_changes,
            version=new_datapackage.version,
            previous_version=previous_version,
            date=new_datapackage.created,
            previous_version_date=previous_version_date,
        )


def _process_partition_diffs(
    baseline_partitions: dict[str, Any], new_partitions: dict[str, Any]
) -> list[PartitionDiff]:
    all_partition_keys = {*baseline_partitions.keys(), *new_partitions.keys()}
    partition_diffs = []
    for key in all_partition_keys:
        baseline_val = baseline_partitions.get(key)
        new_val = new_partitions.get(key)

        match baseline_val, new_val:
            case None, created_part_val:
                partition_diffs.append(
                    PartitionDiff(
                        key=key,
                        value=created_part_val,
                        diff_type="CREATE",
                    )
                )
            case deleted_part_val, None:
                partition_diffs.append(
                    PartitionDiff(
                        key=key,
                        previous_value=deleted_part_val,
                        diff_type="DELETE",
                    )
                )
            case old_val, new_val if old_val != new_val:
                partition_diffs.append(
                    PartitionDiff(
                        key=key,
                        value=new_val,
                        previous_value=old_val,
                        diff_type="UPDATE",
                    )
                )

    return partition_diffs


def _process_resource_diffs(
    baseline_resources: dict[str, Resource], new_resources: dict[str, Resource]
) -> list[FileDiff]:
    baseline_set = set(baseline_resources.keys())
    new_set = set(new_resources.keys())

    resource_overlap = baseline_set.intersection(new_set)
    created_resources = new_set - baseline_set
    deleted_resources = baseline_set - new_set

    created_resources = [
        FileDiff(
            name=resource, diff_type="CREATE", size_diff=new_resources[resource].bytes_
        )
        for resource in created_resources
    ]

    deleted_resources = [
        FileDiff(
            name=resource,
            diff_type="DELETE",
            size_diff=-baseline_resources[resource].bytes_,
        )
        for resource in deleted_resources
    ]

    # Get changed resources and partitions
    changed_resources = []
    for resource in resource_overlap:
        file_changed = (
            baseline_resources[resource].hash_ != new_resources[resource].hash_
        )

        baseline_resource = baseline_resources[resource]
        new_resource = new_resources[resource]

        partition_diffs = _process_partition_diffs(
            baseline_resource.parts, new_resource.parts
        )

        if file_changed or (len(partition_diffs) > 0):
            changed_resources.append(
                FileDiff(
                    name=resource,
                    diff_type="UPDATE",
                    size_diff=new_resource.bytes_ - baseline_resource.bytes_,
                    partition_changes=partition_diffs,
                )
            )

    return [*changed_resources, *created_resources, *deleted_resources]

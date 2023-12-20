"""Defines models used for validating/summarizing an archiver run."""
import logging
import xml.etree.ElementTree as Et  # nosec: B405
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel

from pudl_archiver.frictionless import DataPackage, Resource, ZipLayout
from pudl_archiver.utils import Url

logger = logging.getLogger(f"catalystcoop.{__name__}")


class ValidationTestResult(BaseModel):
    """Class containing results of a validation test, and metdata about the test."""

    name: str
    description: str
    required_for_run_success: bool = True
    success: bool
    notes: list[
        str
    ] | None = None  # Optional note to provide details like why test failed

    # Flag to allow ignoring tests that pass to avoid cluttering the summary
    always_serialize_in_summary: bool = True


class DatasetSpecificValidation(ValidationTestResult):
    """ValidationTestResult specific to an entire dataset."""


class FileSpecificValidation(ValidationTestResult):
    """ValidationTestResult specific to a single file."""

    resource_name: Path
    always_serialize_in_summary: bool = False


def validate_filetype(
    path: Path, required_for_run_success: bool
) -> FileSpecificValidation:
    """Check that file is valid based on type."""
    return FileSpecificValidation(
        name="Valid Filetype Test",
        description="Check that file appears to be valid based on it's extension.",
        required_for_run_success=required_for_run_success,
        resource_name=path,
        success=_validate_file_type(path, BytesIO(path.read_bytes())),
    )


def validate_file_not_empty(
    path: Path, required_for_run_success: bool
) -> FileSpecificValidation:
    """Check that file is valid based on type."""
    return FileSpecificValidation(
        name="Empty File Test",
        description="Check that file is not empty.",
        required_for_run_success=required_for_run_success,
        resource_name=path,
        success=path.stat().st_size > 0,
    )


def validate_zip_layout(
    path: Path, layout: ZipLayout | None, required_for_run_success: bool
) -> FileSpecificValidation:
    """Check that file is valid based on type."""
    if layout is not None:
        valid_layout, layout_notes = layout.validate_zip(path)
    else:
        valid_layout, layout_notes = True, []

    return FileSpecificValidation(
        name="Zipfile Layout Test",
        description="Check that the internal layout of a zipfile is as expected.",
        required_for_run_success=required_for_run_success,
        resource_name=path,
        success=valid_layout,
        notes=layout_notes,
    )


class PartitionDiff(BaseModel):
    """Model summarizing changes in partitions."""

    key: Any = None
    value: str | int | list[str | int] | None = None
    previous_value: str | int | list[str | int] | None = None
    diff_type: Literal["CREATE", "UPDATE", "DELETE"]


class FileDiff(BaseModel):
    """Model summarizing changes to a single file in a deposition."""

    name: str
    diff_type: Literal["CREATE", "UPDATE", "DELETE"]
    size_diff: int
    partition_changes: list[PartitionDiff] = []


class RunSummary(BaseModel):
    """Model summarizing results of an archiver run that can be easily output as JSON."""

    dataset_name: str
    validation_tests: list[ValidationTestResult]
    file_changes: list[FileDiff]
    version: str = ""
    previous_version: str = ""
    date: str
    previous_version_date: str
    record_url: Url

    @property
    def success(self) -> bool:
        """Return True if all tests marked as ``required_for_run_success`` passed."""
        test_results = [
            (test.success or not test.required_for_run_success)
            for test in self.validation_tests
        ]
        return all(test_results)

    @classmethod
    def create_summary(
        cls,
        name: str,
        baseline_datapackage: DataPackage | None,
        new_datapackage: DataPackage,
        validation_tests: list[ValidationTestResult],
        record_url: Url,
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
            validation_tests=[
                test
                for test in validation_tests
                if (not test.success) or test.always_serialize_in_summary
            ],
            file_changes=file_changes,
            version=new_datapackage.version,
            previous_version=previous_version,
            date=new_datapackage.created,
            previous_version_date=previous_version_date,
            record_url=record_url,
        )


def _process_partition_diffs(
    baseline_partitions: dict[str, Any], new_partitions: dict[str, Any]
) -> list[PartitionDiff]:
    """Summarize how partitions have changed."""
    all_partition_keys = {*baseline_partitions.keys(), *new_partitions.keys()}
    partition_diffs = []
    for key in all_partition_keys:
        baseline_val = baseline_partitions.get(key)
        new_val = new_partitions.get(key)

        match baseline_val, new_val:
            case [None, created_part_val]:
                partition_diffs.append(
                    PartitionDiff(
                        key=key,
                        value=created_part_val,
                        diff_type="CREATE",
                    )
                )
            case [deleted_part_val, None]:
                partition_diffs.append(
                    PartitionDiff(
                        key=key,
                        previous_value=deleted_part_val,
                        diff_type="DELETE",
                    )
                )
            case [old_val, new_val] if old_val != new_val:
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
    """Check how resources have changed."""
    # Get sets of resources from previous version and new version
    baseline_set = set(baseline_resources.keys())
    new_set = set(new_resources.keys())

    # Compare sets
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

        # Consider resource to have changed if the file hash or partitions have changed
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


def _validate_file_type(path: Path, buffer: BytesIO) -> bool:
    """Check that file appears valid based on extension."""
    extension = path.suffix

    if extension == ".zip" or extension == ".xlsx":
        return zipfile.is_zipfile(buffer)

    if extension == ".xml" or extension == ".xbrl" or extension == ".xsd":
        return _validate_xml(buffer)

    logger.warning(f"No validations defined for files of type: {extension} - {path}")
    return True


def _validate_xml(buffer: BytesIO) -> bool:
    try:
        Et.parse(buffer)  # noqa: S314
    except Et.ParseError:
        return False

    return True


def validate_data_continuity(new_datapackage: DataPackage) -> DatasetSpecificValidation:
    """Check that the archived data are continuous and complete."""
    description = "Test that data are continuous and complete"
    success = True
    note = ""
    part_range_dict = {"year_quarter": [1, 4, 7, 10], "year_month": list(range(1, 13))}
    # Make a dictionary of part type and parts (ex: {"quarter_year": [1995q1, 1995q2]}) from the new
    # archive datapackage to make it easier to loop through.
    parts_dict_list = [resource.parts for resource in new_datapackage.resources]
    # Identify the newest year for the whole archive
    newest_year_part = max(
        [
            pd.to_datetime(x).year
            for y in [value for item in parts_dict_list for value in item.values()]
            for x in y
        ]
    )
    # Loop through each resource to make sure it's partitions appear as expected.
    for resource in new_datapackage.resources:
        part_label = [x for x, y in resource.parts.items()][0]
        date_list = [pd.to_datetime(x) for x in resource.parts[part_label]]
        date_list.sort()
        date_list_months = [date.month for date in date_list]
        date_list_years = [date.year for date in date_list]
        # Make sure each partition only contains one year of data.
        if len(set(date_list_years)) > 1:
            success = False
            note = (
                note
                + f"Partition contains more than one year: {set(date_list_years)}. "
            )
        # If it's not the newest year of data, make sure all expected months are there.
        if date_list_years[0] != newest_year_part:
            if date_list_months != part_range_dict[part_label]:
                success = False
                note = note + (
                    f"Resource paritions: {date_list_months} do not match expected partitions: \
                        {part_range_dict[part_label]} for {part_label} partitions. "
                )
        # If it is the newest year of data, make sure the records are consecutive.
        # (i.e., not missing a quarter or month).
        elif part_range_dict[part_label][: len(date_list_months)] != date_list_months:
            success = False
            note = note + (
                f"Resource partitions from the most recent year: \
                    {date_list_months} do not match expected partitions: \
                    {part_range_dict[part_label][:len(date_list_months)]}. "
            )
    return {
        "name": "validate_data_continuity",
        "description": description,
        "success": success,
        "note": note,
    }

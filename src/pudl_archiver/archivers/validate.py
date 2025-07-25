"""Defines models used for validating/summarizing an archiver run."""

import logging
import re
import xml.etree.ElementTree as Et  # nosec: B405
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel

from pudl_archiver.frictionless import DataPackage, Resource, ZipLayout
from pudl_archiver.utils import Url, is_html_file

logger = logging.getLogger(f"catalystcoop.{__name__}")


class ValidationTestResult(BaseModel):
    """Class containing results of a validation test, and metdata about the test."""

    name: str
    description: str
    required_for_run_success: bool = True
    success: bool
    notes: list[str] = []  # Optional note to provide details like why test failed

    # Flag to allow ignoring tests that pass to avoid cluttering the summary
    always_serialize_in_summary: bool = True


class DatasetUniversalValidation(ValidationTestResult):
    """ValidationTestResult applied to an entire dataset for all data sources."""


class FileUniversalValidation(ValidationTestResult):
    """ValidationTestResult applied to a single file for all data sources."""

    resource_name: Path


def validate_filetype(
    path: Path, required_for_run_success: bool
) -> FileUniversalValidation:
    """Check that file is valid based on type."""
    return FileUniversalValidation(
        name="Valid Filetype Test",
        description="Check that all files appear to be valid based on their extensions.",
        required_for_run_success=required_for_run_success,
        resource_name=path,
        success=_validate_file_type(path, BytesIO(path.read_bytes())),
        notes=[path.name],
    )


def validate_file_not_empty(
    path: Path, required_for_run_success: bool
) -> FileUniversalValidation:
    """Check that file is valid based on type."""
    return FileUniversalValidation(
        name="Empty File Test",
        description="Check that files are not empty.",
        required_for_run_success=required_for_run_success,
        resource_name=path,
        success=path.stat().st_size > 0,
        notes=[path.name],
    )


def validate_zip_layout(
    path: Path, layout: ZipLayout | None, required_for_run_success: bool
) -> FileUniversalValidation:
    """Check that file is valid based on type."""
    if layout is not None:
        valid_layout, layout_notes = layout.validate_zip(path)
    else:
        valid_layout, layout_notes = True, []

    return FileUniversalValidation(
        name="Zipfile Layout Test",
        description="Check that the internal layout of zipfiles are as expected.",
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
    datapackage_changed: bool

    def get_failed_tests(self) -> list[ValidationTestResult]:
        """Return any tests that failed."""
        return [test for test in self.validation_tests if not test.success]

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
        datapackage_changed = True
        if baseline_datapackage is not None:
            baseline_resources = {
                resource.name: resource for resource in baseline_datapackage.resources
            }
            datapackage_changed = _datapackage_changed(
                baseline_datapackage, new_datapackage
            )

        new_resources = {
            resource.name: resource for resource in new_datapackage.resources
        }

        file_changes = _process_resource_diffs(baseline_resources, new_resources)
        file_changes = sorted(file_changes, key=lambda d: d.name)  # Sort by filename

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
            datapackage_changed=datapackage_changed,
        )


def _datapackage_changed(
    baseline_datapackage: DataPackage,
    new_datapackage: DataPackage,
) -> bool:
    """Check if any fields in datapackage have changed."""
    # Copy datapackages so we can modify without causing problems down the line
    new_datapackage_copy = new_datapackage.model_copy(deep=True)
    old_datapackage_copy = baseline_datapackage.model_copy(deep=True)
    for field in new_datapackage_copy.model_dump():
        if field in {"created", "version"}:
            continue
        if field == "resources":
            for r in old_datapackage_copy.resources + new_datapackage_copy.resources:
                r.path = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.path))
        if getattr(new_datapackage_copy, field) != getattr(old_datapackage_copy, field):
            return True
    return False


def _process_partition_diffs(
    baseline_partitions: dict[str, Any], new_partitions: dict[str, Any]
) -> list[PartitionDiff]:
    """Summarize how partitions have changed."""
    all_partition_keys = {*baseline_partitions.keys(), *new_partitions.keys()}
    partition_diffs = []
    for key in all_partition_keys:
        baseline_val = baseline_partitions.get(key)
        new_val = new_partitions.get(key)

        # Sort partitions if list or set
        if isinstance(baseline_val, list | set):
            baseline_val = sorted(baseline_val)
        if isinstance(new_val, list | set):
            new_val = sorted(new_val)

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


def _validate_file_type(path: Path, buffer: BytesIO) -> bool:  # noqa:C901
    """Check that file appears valid based on extension."""
    extension = path.suffix

    if extension == ".xlsx":
        return zipfile.is_zipfile(buffer)

    if extension == ".zip":
        if zipfile.is_zipfile(buffer):
            try:
                zip_test = zipfile.ZipFile(buffer).testzip()
                return zip_test is None  # None if no error
            except NotImplementedError:
                logger.warn(
                    f"File {path} has a type of zip compression that isn't supported for validation."
                )
                return True
        return False

    if extension == ".xml" or extension == ".xbrl" or extension == ".xsd":
        return _validate_xml(buffer)

    if extension == ".pdf":
        header = buffer.read(5)
        buffer.seek(0)
        return header.startswith(b"%PDF-")

    if extension == ".parquet":
        return _validate_parquet(buffer)

    if extension == ".csv":
        return _validate_csv(buffer)

    if extension == ".xls":
        header = buffer.read(8)
        buffer.seek(0)
        # magic bytes for old-school xls file
        return header.hex() == "d0cf11e0a1b11ae1"

    if extension == ".html":
        return is_html_file(buffer)

    if extension == ".txt":
        return _validate_text(buffer)

    logger.warning(f"No validations defined for files of type: {extension} - {path}")
    return True


def _validate_xml(buffer: BytesIO) -> bool:
    try:
        Et.parse(buffer)  # noqa: S314
    except Et.ParseError:
        return False
    return True


def _validate_csv(buffer: BytesIO) -> bool:
    try:
        sliver = pd.read_csv(buffer, nrows=100)  # Try reading in a data slice
        return not sliver.empty
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return False
    return True


def _validate_parquet(buffer: BytesIO) -> bool:
    try:
        pa.parquet.ParquetFile(buffer)
        return True
    except (pa.lib.ArrowInvalid, pa.lib.ArrowException):
        return False


def _validate_text(buffer: BytesIO) -> bool:
    """Try decoding as UTF-8, then as Latin-1."""
    sample = buffer.read(1_000_000)
    buffer.seek(0)
    try:
        sample.decode(encoding="utf-8")
        return True
    except UnicodeDecodeError:
        try:
            sample.decode(encoding="latin-1")
            return True
        except UnicodeDecodeError:
            return False

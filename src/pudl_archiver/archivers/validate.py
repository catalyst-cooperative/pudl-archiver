"""Defines models used for validating/summarizing an archiver run."""

import datetime
import json
import logging
import re
import xml.etree.ElementTree as Et  # nosec: B405
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from pudl_archiver.frictionless import (
    DataPackage,
    HttpFileMetadata,
    Partitions,
    Resource,
    ZipLayout,
)
from pudl_archiver.utils import RunSettings, Url, is_html_file

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


def exception_validation(e: Exception | None) -> DatasetUniversalValidation:
    """Represents whether an exception was encountered while downloading / uploading resources."""
    return DatasetUniversalValidation(
        name="Run exception validation test",
        description="Represents whether an exception was encountered while downloading / uploading resources.",
        required_for_run_success=True,
        success=e is None,
        notes=[
            f"Encountered the following exception while downloading resources and adding them to the deposition: {e}"
            if e is not None
            else "Run completed without error!"
        ],
    )


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
    """Check that file is not empty."""
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


class LastModifiedCheck(BaseModel):
    """Test of whether HTTP metadata predicts that a file has changed.

    Before looking at the downloaded file, we predict whether it changed since we
    last archived it using only the metadata that the server sent with it: it has
    changed if any of the values that we can compare differs from what we recorded.
    Afterwards we compare the prediction with whether the hash of the file actually
    changed.
    """

    name: str
    last_modified: datetime.datetime | None
    etag: str | None
    content_length: int | None
    #: What the server metadata was compared with: ``stored_metadata`` (recorded in
    #: the previous datapackage), ``previous_upload_time`` (when the previous copy
    #: was uploaded), ``new_file`` or ``none`` (no way to predict).
    reference: Literal["stored_metadata", "previous_upload_time", "new_file", "none"]
    reference_time: datetime.datetime | None = None
    predicted_changed: bool | None
    actually_changed: bool

    @property
    def prediction_correct(self) -> bool | None:
        """Whether the prediction matched reality, or None if there was none."""
        if self.predicted_changed is None:
            return None
        return self.predicted_changed == self.actually_changed


def create_last_modified_checks(
    observed: dict[str, HttpFileMetadata],
    baseline_resources: dict[str, Resource],
    new_resources: dict[str, Resource],
    previous_upload_times: dict[str, datetime.datetime],
    etag_is_content_hash: bool = False,
) -> list[LastModifiedCheck]:
    """Compare predictions of changes from HTTP metadata with actual changes.

    A file is predicted to have changed if any of the following differs:

    - its size from the size of the archived file, which is exactly what the server
      sent, so a different size proves that the file has changed,
    - its ``Last-Modified`` from the one recorded with the archived file, or if none
      was recorded, from when the archived file was uploaded, and
    - its ``ETag`` from the recorded one, but only if ``etag_is_content_hash``.

    Args:
        observed: HTTP metadata of each file downloaded in this run.
        baseline_resources: Resources in the previous version of the archive.
        new_resources: Resources in the new version of the archive.
        previous_upload_times: When each file in the previous version was uploaded.
            Used for files whose metadata wasn't recorded in the previous version.
        etag_is_content_hash: Whether to compare ETags, as the server makes them
            from the contents of the file.
    """
    fields = ["last_modified", *(["etag"] if etag_is_content_hash else [])]
    checks = []
    for name, metadata in sorted(observed.items()):
        baseline = baseline_resources.get(name)
        new = new_resources.get(name)
        if new is None:
            continue
        actually_changed = baseline is None or baseline.hash_ != new.hash_

        reference_time = None
        if baseline is None:
            reference, predicted = "new_file", True
        else:
            differs = []
            if metadata.content_length is not None:
                differs.append(metadata.content_length != baseline.bytes_)
            if (stored := baseline.source_metadata) is not None:
                reference, reference_time = "stored_metadata", stored.last_modified
                differs += [
                    getattr(stored, field) != getattr(metadata, field)
                    for field in fields
                    if getattr(stored, field) is not None
                    and getattr(metadata, field) is not None
                ]
            elif metadata.last_modified is not None and name in previous_upload_times:
                reference = "previous_upload_time"
                reference_time = previous_upload_times[name]
                differs.append(metadata.last_modified > reference_time)
            else:
                reference = "none"
            predicted = any(differs) if differs else None

        checks.append(
            LastModifiedCheck(
                name=name,
                last_modified=metadata.last_modified,
                etag=metadata.etag,
                content_length=metadata.content_length,
                reference=reference,
                reference_time=reference_time,
                predicted_changed=predicted,
                actually_changed=actually_changed,
            )
        )
    return checks


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
    failed_partitions: dict[str, Partitions]
    successful_partitions: dict[str, Partitions]
    #: MD5 checksum, by filename, of each file of the ``successful_partitions`` as it
    #: was in the draft at the end of the run. A retry only skips a file that is
    #: still in the draft with this checksum.
    uploaded_checksums: dict[str, str] = {}
    #: HTTP metadata, by filename, of the files of the ``successful_partitions``, so
    #: that a retry, or a publish, can record it too for the files it doesn't download.
    source_metadata: dict[str, HttpFileMetadata] = {}
    run_settings: RunSettings
    last_modified_checks: list[LastModifiedCheck] = []

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
        failed_partitions: dict[str, Partitions],
        successful_partitions: dict[str, Partitions],
        run_settings: RunSettings,
        uploaded_checksums: dict[str, str] | None = None,
        last_modified_checks: list[LastModifiedCheck] | None = None,
        source_metadata: dict[str, HttpFileMetadata] | None = None,
    ) -> RunSummary:
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
            failed_partitions=failed_partitions,
            successful_partitions=successful_partitions,
            uploaded_checksums=uploaded_checksums or {},
            source_metadata=source_metadata or {},
            run_settings=run_settings,
            last_modified_checks=last_modified_checks or [],
        )

    @classmethod
    def load_previous_run(
        cls, summary_file: str | Path, auto_publish: bool
    ) -> RunSummary:
        """Create a RunSummary object from a JSON file output by a previous run.

        Load settings from summary file, but always override ``auto_publish``
        to avoid accidental publication.

        Args:
            summary_file: Points to JSON file containing serialized RunSummary metadata.
            auto_publish: Explicitly set ``auto_publish``.
        """
        # Load run summary file and parse
        with Path(summary_file).open() as f:
            previous_run_summary = RunSummary.model_validate(json.load(f))

        # Extract settings from failed run
        return previous_run_summary.model_copy(
            update={
                "run_settings": previous_run_summary.run_settings.model_copy(
                    update={
                        "retry_run": summary_file,
                        "auto_publish": auto_publish,
                    },
                )
            }
        )


class MetadataArchiveSummary(BaseModel):
    """Summary of archiving a dataset's ``datapackage.json`` alone to Zenodo.

    Shares the ``dataset_name``, ``record_url``, ``validation_tests`` and
    ``file_changes`` keys with ``RunSummary`` so it can be read by the same
    notification tooling, which uses ``metadata_only`` to tell them apart.
    """

    dataset_name: str
    metadata_only: bool = True
    record_url: Url
    version: str
    doi: Url
    datapackage_changed: bool
    validation_tests: list[ValidationTestResult] = []
    file_changes: list[FileDiff] = []


def _datapackage_changed(
    baseline_datapackage: DataPackage,
    new_datapackage: DataPackage,
) -> bool:
    """Check if any fields in datapackage have changed."""
    # Copy datapackages so we can modify without causing problems down the line
    new_datapackage_copy = new_datapackage.model_copy(deep=True)
    old_datapackage_copy = baseline_datapackage.model_copy(deep=True)
    for field in new_datapackage_copy.model_dump():
        if field in {"created", "version", "id_"}:
            continue
        if field == "resources":
            for r in old_datapackage_copy.resources + new_datapackage_copy.resources:
                r.path = re.sub(r"/\d+/", "/ID_NUMBER/", str(r.path))
                # Server metadata is informational, a change alone isn't a change
                r.source_metadata = None
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
                logger.warning(
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
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as _:
        return False
    return True


def _validate_parquet(buffer: BytesIO) -> bool:
    try:
        pq.ParquetFile(buffer)
        return True
    except (pa.lib.ArrowInvalid, pa.lib.ArrowException) as _:
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

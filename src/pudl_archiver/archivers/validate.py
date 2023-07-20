"""Defines models used for validating/summarizing an archiver run."""
from typing import Literal

from pydantic import BaseModel


class ValidationTestResult(BaseModel):
    """Class containing results of a validation test, and metdata about the test."""

    name: str
    description: str
    ignore_failure: bool = False
    resource_name: str | None = None  # If test is specific to a single resource
    success: bool
    note: str | None = None  # Optional note to provide details like why test failed


class FileDiff(BaseModel):
    """Model summarizing changes to a single file in a deposition."""

    name: str
    diff_type: Literal["CREATE", "UPDATE", "DELETE"]
    size_diff: int


class RunSummary(BaseModel):
    """Model summarizing results of an archiver run that can be easily output as JSON."""

    dataset_name: str
    validation_tests: list[ValidationTestResult]
    file_changes: list[FileDiff]
    version: str
    pervious_version: str
    date: str
    previous_version_date: str

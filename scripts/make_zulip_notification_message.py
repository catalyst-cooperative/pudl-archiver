#! /usr/bin/env python
"""Format summary files for Zulip perusal. Only uses stdlib.

Outputs a Markdown string suitable for sending to Zulip via
the zulip/github-actions-zulip/send-message action.
"""

import argparse
import json
import logging
import re
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary-files",
        nargs="+",
        type=Path,
        help="Paths to RunSummary JSON files.",
        default=None,
    )
    parser.add_argument(
        "--error-files",
        nargs="+",
        type=Path,
        help="Paths to log files for failed runs.",
        default=None,
    )
    parser.add_argument(
        "--workflow-url",
        type=str,
        help="URL of the GitHub Actions workflow run.",
        default=None,
    )
    return parser.parse_args()


def _format_entry(url: str | None, name: str, content: str) -> str:
    """Format a single message entry for Zulip.

    When archives fail, they may not have a URL.
    """
    if url:
        return f"[**{name}**]({url})\n\n{content}"
    return f"**{name}**\n\n{content}"


def _format_failures(summary: dict) -> str | None:
    name = summary["dataset_name"]
    url = summary["record_url"]

    test_failures = defaultdict(list)
    for validation_test in summary["validation_tests"]:
        if (not validation_test["success"]) and (
            validation_test["required_for_run_success"]
        ):
            test_failures = ". ".join(
                [validation_test["name"]] + validation_test["notes"]
            )

    if test_failures:
        failures = f"```\n{json.dumps(test_failures, indent=2)}\n```"
    else:
        return None

    return _format_entry(url=url, name=name, content=failures)


def _format_summary(summary: dict) -> str | None:
    name = summary["dataset_name"]
    url = summary["record_url"]
    if any(not test["success"] for test in summary["validation_tests"]):
        return None  # Don't report on file changes if any test failed.

    if file_changes := summary["file_changes"]:
        abridged_changes = defaultdict(list)
        for change in file_changes:
            abridged_changes[change["diff_type"]].append(
                # Convert the size diff to MB for speed of assessment, and round
                {change["name"]: round(change["size_diff"] * 1e-6, 4)}
            )
        changes = f"```\n{json.dumps(abridged_changes, indent=2)}\n```"
    else:
        changes = "No changes."

    return _format_entry(url=url, name=name, content=changes)


def _format_errors(log: str) -> str | None:
    """Take a log file from a failed run and return the exception."""
    failure_match = list(re.finditer("Traceback", log))
    if not failure_match or any(
        "archive validation tests failed" in log[failure.start() :]
        for failure in failure_match
    ):
        # We already capture archive validation failures elsewhere, so ignore these.
        return None
    # Get last traceback
    failure = log[failure_match[-1].start() :]
    # Keep last three lines to get a sliver of the error message
    failure = "\n".join(failure.splitlines()[-3:])
    failure = f"```\n{failure}\n```"  # Format as code

    name_re = re.search(
        r"(?:catalystcoop.pudl_archiver.archivers.classes:\d+ Archiving )([a-z0-9]*)",
        log,
    )
    name = name_re.group(1) if name_re else "Unknown"

    url_re = re.search(
        r"(?:INFO:catalystcoop.pudl_archiver.depositors.zenodo.depositor:PUT )(https:\/\/[a-z0-9\/.]*)",
        log,
    )
    url = url_re.group(1) if url_re else None

    return _format_entry(url=url, name=name, content=failure)


def _load_summaries(summary_files: list[Path]) -> list[dict]:
    summaries = []
    for summary_file in summary_files:
        if summary_file.exists():
            with summary_file.open() as f:
                summaries.append(json.loads(f.read()))
    return summaries


def _load_errors(error_files: list[Path]) -> list[str]:
    errors = []
    for error_file in error_files:
        if error_file.exists():
            with error_file.open() as f:
                errors.append(f.read())
    return errors


def main(
    summary_files: list[Path], error_files: list[Path], workflow_url: str | None = None
) -> None:
    """Format summary files for Zulip perusal."""
    summaries = _load_summaries(summary_files)
    errors = _load_errors(error_files)

    error_entries = list(filter(None, (_format_errors(e) for e in errors)))
    failed_entries = list(filter(None, (_format_failures(s) for s in summaries)))
    changed_entries = list(
        filter(None, (_format_summary(s) for s in summaries if s["file_changes"]))
    )
    unchanged_entries = list(
        filter(None, (_format_summary(s) for s in summaries if not s["file_changes"]))
    )

    parts = ["### PUDL data archive run complete."]

    if workflow_url:
        parts.append(f"[View workflow run]({workflow_url})")

    parts.append("## Archiver Run Outcomes")

    if error_entries:
        parts.append("### Run Failures")
        parts.append("\n---\n".join(error_entries))

    if failed_entries:
        parts.append("### Validation Failures")
        parts.append("\n---\n".join(failed_entries))

    if changed_entries:
        parts.append("### Changed")
        parts.append("\n---\n".join(changed_entries))

    if unchanged_entries:
        parts.append("### Unchanged")
        parts.append("\n---\n".join(unchanged_entries))

    print("\n\n".join(parts))


if __name__ == "__main__":
    main(**vars(_parse_args()))

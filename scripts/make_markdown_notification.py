#! /usr/bin/env python
"""Format summary and error files into Github issue template.

Creates chunks that can be added to the Github issue template
`monthly-archiver-update.yml`. Only uses stdlib.

Sets a series of env variables to put into the Slack Github Action:

https://github.com/slackapi/slack-github-action

Which follows the attachments format (see
https://api.slack.com/methods/chat.postMessage#arg_attachments) in the Slack API
- see the Block Kit Builder (https://app.slack.com/block-kit-builder/) for an
interactive playground for the API.

We stuff everything into an attachment because that lets us automatically
hide large messages (such as a file diff) behind a "See more" action.
"""

import argparse
import json
import logging
import re
from pathlib import Path

import pandas as pd

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
        "--summary-type",
        type=str,
        help="What category of text to get (changes, failures, zulip).",
        default=None,
    )
    parser.add_argument(
        "--workflow-url",
        type=str,
        help="URL of the GitHub Actions workflow run for Zulip notifications.",
        default=None,
    )
    return parser.parse_args()


def _format_message(
    url: str | None,
    name: str,
    content: str,
    action: str | None = None,
    include_action: bool = True,
) -> str:
    """Format message for Markdown with the dataset, its URL and action items.

    When archives fail, they may not have a URL.
    """
    header = f"### [{name}]({url})" if url else f"### {name}"

    if action and include_action:
        action_prefix = "- [ ]" if include_action else "-"
        return f"{header}\n\n{content}\n\n{action_prefix} {action}"
    return f"{header}\n\n{content}"


def _format_text_as_github_code(text: str) -> str:
    """Set up code to render nicely in one block, instead of per-line."""
    return f"```\n{text}\n```"


def _format_failures(
    summary: dict,
    include_action: bool = True,
) -> str | None:
    name = summary["dataset_name"]
    url = summary["record_url"]

    test_failures = []
    for validation_test in summary["validation_tests"]:
        if (not validation_test["success"]) and (
            validation_test["required_for_run_success"]
        ):
            failure_text = validation_test["name"]
            if validation_test.get("notes"):
                failure_text += ": " + ". ".join(validation_test["notes"])
            test_failures.append(f"- {failure_text}")

    if test_failures:
        failures = "\n".join(test_failures)
    else:
        return None

    return _format_message(
        url=url,
        name=name,
        content=failures,
        action="Investigate failure",
        include_action=include_action,
    )


def _format_summary(
    summary: dict,
    include_action: bool = True,
) -> str | None:
    name = summary["dataset_name"]
    url = summary["record_url"]
    if any(not test["success"] for test in summary["validation_tests"]):
        return None  # Don't report on file changes if any test failed.

    if file_changes := summary["file_changes"]:
        file_change_table = pd.DataFrame.from_records(file_changes)
        file_change_table["size_diff"] = round(file_change_table["size_diff"] * 1e-6, 4)
        file_change_table = file_change_table.rename(
            columns={"size_diff": "change_in_mb"}
        )
        file_change_table["partition_changes"] = (
            file_change_table["partition_changes"].astype(str).replace("[]", "")
        )  # Replace no partition change with empty string
        # Convert to Markdown table
        changes = file_change_table.to_markdown(index=False)
        action = "Reviewed and published to Zenodo"
    else:
        # If no changes, don't specify an action.
        changes = "No changes."
        action = None

    return _format_message(
        url=url,
        name=name,
        content=changes,
        action=action,
        include_action=include_action,
    )


def _format_errors(
    log: str,
    include_action: bool = True,
) -> str | None:
    """Take a log file from a failed run and return the exception."""
    # First isolate traceback
    failure_match = list(re.finditer("Traceback", log))

    if not failure_match or any(
        validation_message in log[failure.start() :]
        for failure in failure_match
        for validation_message in [
            "Archive validation failed",
            "archive validation tests failed",
        ]
    ):
        # We already capture archive validation failures elsewhere, so ignore these.
        return None
    # Get last traceback
    failure = log[failure_match[-1].start() :]
    # Keep last three lines to get a sliver of the error message
    failure = "\n".join(failure.splitlines()[-3:])
    failure = _format_text_as_github_code(failure)  # Format as code

    name_re = re.search(
        r"(?:catalystcoop.pudl_archiver.archivers.classes:\d+ Archiving )([a-z0-9]*)",
        log,
    )
    name = name_re.group(1) if name_re else "Unknown"

    # TODO: Change to link to the Github job URL when they make the Job ID accessible
    # from a given job's context
    url_re = re.search(
        r"(?:INFO:catalystcoop.pudl_archiver.depositors.zenodo.depositor:PUT )(https:\/\/[a-z0-9\/.]*)",
        log,
    )
    # If an archiver doesn't make it to this stage, return nothing and don't make this
    # a hyperlink.
    url = url_re.group(1) if url_re else None

    return _format_message(
        url=url,
        name=name,
        content=failure,
        action="Investigate error",
        include_action=include_action,
    )


def _build_markdown_report(
    error_blocks: str,
    failed_blocks: str,
    changed_blocks: str,
    unchanged_blocks: str,
    workflow_url: str | None = None,
    title: str | None = None,
) -> str:
    """Build a single Markdown report from the formatted summary blocks."""
    parts: list[str] = []

    if title:
        parts.append(title)

    if workflow_url:
        parts.append(f"[View workflow run]({workflow_url})")

    parts.append("# Archiver Run Outcomes")

    if error_blocks:
        parts.append("## Run Failures")
        parts.append(error_blocks)

    if failed_blocks:
        parts.append("## Validation Failures")
        parts.append(failed_blocks)

    if changed_blocks:
        parts.append("## Changed")
        parts.append(changed_blocks)

    if unchanged_blocks:
        parts.append("## Unchanged")
        parts.append(unchanged_blocks)

    if not any([error_blocks, failed_blocks, changed_blocks, unchanged_blocks]):
        parts.append("_No downloaded summary or error artifacts were found._")

    return "\n\n".join(parts)


def _load_summaries(summary_files: list[Path]) -> list[dict]:
    summaries = []
    for summary_file in summary_files:
        if summary_file.exists():  # Handle case where no files are found
            with summary_file.open() as f:
                summaries.append(json.loads(f.read()))
    return summaries


def _load_errors(error_files: list[Path]) -> list[str]:
    errors = []
    for error_file in error_files:
        if error_file.exists():  # Handle case where no files are found or file is empty
            with error_file.open() as f:
                errors.append(f.read())
    return errors


def main(
    summary_files: list[Path],
    error_files: list[Path],
    summary_type: str,
    workflow_url: str | None = None,
) -> None:
    """Format summary files for GitHub issue text or Zulip Markdown."""
    summaries = _load_summaries(summary_files)
    errors = _load_errors(error_files)
    include_action = summary_type != "zulip"

    error_blocks = "\n\n".join(
        filter(
            None,
            (_format_errors(e, include_action) for e in errors),
        )
    )

    failed_blocks = "\n\n".join(
        filter(
            None,
            (_format_failures(s, include_action) for s in summaries),
        )
    )

    unchanged_blocks = "\n\n".join(
        filter(
            None,
            (
                _format_summary(s, include_action)
                for s in summaries
                if not s["file_changes"]
            ),
        )
    )

    changed_blocks = "\n\n".join(
        filter(
            None,
            (
                _format_summary(s, include_action)
                for s in summaries
                if s["file_changes"]
            ),
        )
    )

    if summary_type == "change":
        print(changed_blocks)
    elif summary_type == "error":
        print(error_blocks)
    elif summary_type == "failure":
        print(failed_blocks)
    elif summary_type == "unchanged":
        print(unchanged_blocks)
    elif summary_type == "zulip":
        print(
            _build_markdown_report(
                error_blocks=error_blocks,
                failed_blocks=failed_blocks,
                changed_blocks=changed_blocks,
                unchanged_blocks=unchanged_blocks,
                workflow_url=workflow_url,
                title="# PUDL data archive run complete.",
            )
        )
    else:
        print([changed_blocks, unchanged_blocks, failed_blocks, error_blocks])


if __name__ == "__main__":
    main(**vars(_parse_args()))

#! /usr/bin/env python
"""Format summary files for Slack perusal. Only uses stdlib.

Outputs a JSON payload to put into the Slack Github Action:

https://github.com/slackapi/slack-github-action

Which follows the attachments format (see
https://api.slack.com/methods/chat.postMessage#arg_attachments) in the Slack API
- see the Block Kit Builder (https://app.slack.com/block-kit-builder/) for an
interactive playground for the API.

We stuff everything into an attachment because that lets us automatically
hide large messages (such as a file diff) behind a "See more" action.
"""

import argparse
import itertools
import json
import re
from collections import defaultdict
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary-files", nargs="+", type=Path, help="Paths to RunSummary JSON files."
    )
    parser.add_argument(
        "--error-files",
        nargs="+",
        type=Path,
        help="Paths to log files for failed runs.",
    )
    return parser.parse_args()


def _format_message(
    url: str, name: str, content: str, max_len: int = 3000
) -> list[dict]:
    text = f"<{url}|*{name}*>\n{content}"[:max_len]
    return [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        },
    ]


def _format_failures(summary: dict) -> list[dict]:
    name = summary["dataset_name"]
    url = summary["record_url"]

    test_failures = defaultdict(list)
    for validation_test in summary["validation_tests"]:
        if (not validation_test["success"]) and (
            validation_test["required_for_run_success"]
        ):
            test_failures = ". ".join(
                [validation_test["name"]] + validation_test["notes"]
            )  # Flatten list of lists

    if test_failures:
        failures = f"```\n{json.dumps(test_failures, indent=2)}\n```"
    else:
        return None

    return _format_message(url=url, name=name, content=failures)


def _format_summary(summary: dict) -> list[dict]:
    name = summary["dataset_name"]
    url = summary["record_url"]
    if any(not test["success"] for test in summary["validation_tests"]):
        return None  # Don't report on file changes if any test failed.

    if file_changes := summary["file_changes"]:
        abridged_changes = defaultdict(list)
        for change in file_changes:
            abridged_changes[change["diff_type"]].append(change["name"])
        changes = f"```\n{json.dumps(abridged_changes, indent=2)}\n```"
    else:
        changes = "No changes."

    return _format_message(url=url, name=name, content=changes)


def _format_errors(log: str) -> str:
    """Take a log file from a failed run and return the exception."""
    name_re = re.search(
        r"(?:catalystcoop.pudl_archiver.archivers.classes:155 Archiving )([a-z0-9])*"
    )
    name = name_re.group(1)

    url_re = re.search(
        r"(?:INFO:catalystcoop.pudl_archiver.depositors.zenodo.depositor:PUT )(https:\/\/[a-z0-9\/.]*)",
        log,
    )
    url = url_re.group(1)

    failure = log.splitlines()[-1]
    return _format_message(url=url, name=name, content=failure)


def main(summary_files: list[Path], error_files: list[Path]) -> None:
    """Format summary files for Slack perusal."""
    summaries = []
    errors = []

    for summary_file in summary_files:
        with summary_file.open() as f:
            summaries.extend(json.loads(f.read()))

    for error_file in error_files:
        with error_file.open() as f:
            errors.append(f.read())

    error_blocks = list(
        itertools.chain.from_iterable(
            _format_failures(e) for e in errors if _format_errors(e) is not None
        )
    )

    failed_blocks = list(
        itertools.chain.from_iterable(
            _format_failures(s) for s in summaries if _format_failures(s) is not None
        )
    )

    unchanged_blocks = list(
        itertools.chain.from_iterable(
            _format_summary(s)
            for s in summaries
            if (not s["file_changes"]) and (_format_summary(s) is not None)
        )
    )
    changed_blocks = list(
        itertools.chain.from_iterable(
            _format_summary(s)
            for s in summaries
            if (s["file_changes"]) and (_format_summary(s) is not None)
        )
    )

    def header_block(text: str) -> dict:
        return {"type": "header", "text": {"type": "plain_text", "text": text}}

    def section_block(text: str) -> dict:
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    if error_blocks:
        error_blocks = [section_block("*Run Failures*")] + error_blocks
    if failed_blocks:
        failed_blocks = [section_block("*Validation Failures*")] + failed_blocks
    if changed_blocks:
        changed_blocks = [section_block("*Changed*")] + changed_blocks
    if unchanged_blocks:
        unchanged_blocks = [section_block("*Unchanged*")] + unchanged_blocks

    print(
        json.dumps(
            [header_block("Archiver Run Outcomes")]
            + error_blocks
            + failed_blocks
            + changed_blocks
            + unchanged_blocks,
            indent=2,
        )
    )


if __name__ == "__main__":
    main(**vars(_parse_args()))

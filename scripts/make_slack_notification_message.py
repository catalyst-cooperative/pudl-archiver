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
    return parser.parse_args()


def _format_message(
    url: str | None, name: str, content: str, max_len: int = 3000
) -> list[dict]:
    """Format message for Slack API.

    When archives fail, they may not have a URL.
    """
    if url:
        text = f"<{url}|*{name}*>\n{content}"[:max_len]
    else:
        text = f"*{name}*\n{content}"[:max_len]
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
            abridged_changes[change["diff_type"]].append(
                # Convert the size diff to MB for speed of assessment, and round
                {change["name"]: round(change["size_diff"] * (1**-6), 4)}
            )
        changes = f"```\n{json.dumps(abridged_changes, indent=2)}\n```"
    else:
        changes = "No changes."

    return _format_message(url=url, name=name, content=changes)


def _format_errors(log: str) -> str:
    """Take a log file from a failed run and return the exception."""
    # First isolate traceback
    failure_match = list(re.finditer("Traceback", log))
    if not failure_match or any(
        "RuntimeError: Error: archive validation tests failed." in failure.group()
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
        r"(?:catalystcoop.pudl_archiver.archivers.classes:155 Archiving )([a-z0-9]*)",
        log,
    )
    name = name_re.group(1)

    # TODO: Change to link to the Github job URL when they make the Job ID accessible
    # from a given job's context
    url_re = re.search(
        r"(?:INFO:catalystcoop.pudl_archiver.depositors.zenodo.depositor:PUT )(https:\/\/[a-z0-9\/.]*)",
        log,
    )
    # If an archiver doesn't make it to this stage, return nothing and don't make this
    # a hyperlink.
    url = url_re.group(1) if url_re else None

    return _format_message(url=url, name=name, content=failure)


def _load_summaries(summary_files: list[Path]) -> list[dict]:
    summaries = []
    for summary_file in summary_files:
        if summary_file.exists():  # Handle case where no files are found
            with summary_file.open() as f:
                summaries.extend(json.loads(f.read()))
    return summaries


def _load_errors(error_files: list[Path]) -> list[str]:
    errors = []
    for error_file in error_files:
        if error_file.exists():  # Handle case where no files are found or file is empty
            with error_file.open() as f:
                errors.append(f.read())
    return errors


def main(summary_files: list[Path], error_files: list[Path]) -> None:
    """Format summary files for Slack perusal."""
    summaries = _load_summaries(summary_files)
    errors = _load_errors(error_files)

    error_blocks = list(
        itertools.chain.from_iterable(filter(None, (_format_errors(e) for e in errors)))
    )

    failed_blocks = list(
        itertools.chain.from_iterable(
            filter(None, (_format_failures(s) for s in summaries))
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

    if len(error_blocks + failed_blocks + unchanged_blocks + changed_blocks) >= 50:
        # Slack doesn't let us send more than 50 items in a list
        # Let's squish our unchanged blocks together.

        unchanged_blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n\n".join([s["text"]["text"] for s in unchanged_blocks]),
                },
            },
        ]

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

    blocks = (
        [header_block("Archiver Run Outcomes")]
        + error_blocks
        + failed_blocks
        + changed_blocks
        + unchanged_blocks
    )
    if len(blocks) >= 50:
        logger.warn(
            "Too many archive updates for Slack to handle! Trimming anything over 50."
        )
        blocks = blocks[:49] + [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "And more..."},
            }
        ]

    print(
        json.dumps(
            blocks,
            indent=2,
        )
    )


if __name__ == "__main__":
    main(**vars(_parse_args()))

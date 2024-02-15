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
from collections import defaultdict
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary-files", nargs="+", type=Path, help="Paths to RunSummary JSON files."
    )
    return parser.parse_args()


def main(summary_files: list[Path]) -> None:
    """Format summary files for Slack perusal."""
    summaries = []
    for summary_file in summary_files:
        with summary_file.open() as f:
            summaries.extend(json.loads(f.read()))

    def format_summary(summary: dict) -> list[dict]:
        name = summary["dataset_name"]
        url = summary["record_url"]
        if file_changes := summary["file_changes"]:
            abridged_changes = defaultdict(list)
            for change in sorted(file_changes):
                abridged_changes[change["diff_type"]].append(change["name"])
            changes = f"```\n{json.dumps(abridged_changes, indent=2)}\n```"
        else:
            changes = "No changes."

        max_len = 3000
        text = f"<{url}|*{name}*>\n{changes}"[:max_len]
        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            },
        ]

    unchanged_blocks = list(
        itertools.chain.from_iterable(
            format_summary(s) for s in summaries if not s["file_changes"]
        )
    )
    changed_blocks = list(
        itertools.chain.from_iterable(
            format_summary(s) for s in summaries if s["file_changes"]
        )
    )

    def header_block(text: str) -> dict:
        return {"type": "header", "text": {"type": "plain_text", "text": text}}

    def section_block(text: str) -> dict:
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    if changed_blocks:
        changed_blocks = [section_block("*Changed*")] + changed_blocks
    if unchanged_blocks:
        unchanged_blocks = [section_block("*Unchanged*")] + unchanged_blocks

    print(
        json.dumps(
            {
                "attachments": [
                    {
                        "blocks": [header_block("Archiver Run Outcomes")]
                        + changed_blocks
                        + unchanged_blocks,
                    }
                ]
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main(**vars(_parse_args()))

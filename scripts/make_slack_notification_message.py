#! /usr/bin/env python
"""Format summary files for Slack perusal. Only uses stdlib."""

import argparse
import itertools
import json
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary-files", nargs="+", type=Path, help="Paths to RunSummary JSON files."
    )
    return parser.parse_args()


def main(summary_files: list[Path]):
    """Format summary files for Slack perusal."""
    summaries = []
    for summary_file in summary_files:
        with summary_file.open() as f:
            summaries.extend(json.loads(f.read()))

    def format_summary(summary):
        name = summary["dataset_name"]
        url = summary["record_url"]
        if file_changes := summary["file_changes"]:
            changes = f"```\n{json.dumps(file_changes, indent=2)}\n```"
        else:
            changes = "No changes."
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{url}|*{name}*>\n{changes}",
                },
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

    def header_block(text):
        return {"type": "header", "text": {"type": "plain_text", "text": text}}

    if changed_blocks:
        changed_blocks = [header_block("Changed")] + changed_blocks
    if unchanged_blocks:
        unchanged_blocks = [header_block("Unchanged")] + unchanged_blocks

    print(
        json.dumps(
            {
                "text": "Archiver run completed!",
                "attachments": [{"blocks": changed_blocks + unchanged_blocks}],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main(**vars(_parse_args()))

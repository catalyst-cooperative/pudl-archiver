#! /usr/bin/env python
"""Format summary files for Slack perusal. Only uses stdlib."""

import argparse
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
            changes = f"\n  ```\n{json.dumps(file_changes, indent=2)}\n  ```"
        else:
            changes = " No changes."
        return f"* [{name}]({url}):{changes}"

    unchanged = ["## Unchanged"] + [
        format_summary(s) for s in summaries if len(s["file_changes"]) == 0
    ]
    changed = ["## Changed"] + [
        format_summary(s) for s in summaries if len(s["file_changes"]) > 0
    ]

    print("\n".join(unchanged + changed))


if __name__ == "__main__":
    main(**vars(_parse_args()))

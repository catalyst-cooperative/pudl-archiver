#! /usr/bin/env python
"""Compatibility wrapper for Zulip notification formatting.

Prefer calling ``make_github_notification.py --summary-type zulip`` directly.
"""

import argparse
from pathlib import Path

from make_github_notification import main as make_github_notification_main


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


if __name__ == "__main__":
    args = _parse_args()
    make_github_notification_main(
        summary_files=args.summary_files,
        error_files=args.error_files,
        summary_type="zulip",
        workflow_url=args.workflow_url,
    )

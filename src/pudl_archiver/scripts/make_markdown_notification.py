"""Format archiver summary and error files as Markdown.

This script reads run summary JSON files and optional failure logs, then emits
Markdown suitable for either:

- GitHub issue template sections describing errors, failures, changes, or
    unchanged datasets.
- Zulip notifications containing a full run summary and a workflow-run link.

GitHub outputs include action checkboxes so follow-up work can be tracked in
issues. Zulip outputs omit those actions and serve as notifications only.
"""

import itertools
import json
import logging
import re
from pathlib import Path

import click
import pandas as pd

logger = logging.getLogger(f"catalystcoop.{__name__}")


SUMMARY_TYPES = {
    "error": "Exceptions from the error logs of runs that crashed.",
    "failure": "Validation tests that failed in each run summary.",
    "change": "Tables of files that changed, and Zenodo metadata drafts.",
    "unchanged": "Archives that had no changes.",
    "last_modified": "How well server metadata predicted which files changed (fsspec).",
    "zulip": "A full report of errors, failures, changes and unchanged archives, "
    "for a Zulip notification.",
}


def _format_message(
    url: str | None,
    name: str,
    content: str,
    action: str | None = None,
) -> str:
    """Format message for Markdown with the dataset, its URL and action items.

    When archives fail, they may not have a URL.
    """
    header = f"### [{name}]({url})" if url else f"### {name}"

    if action:
        return f"{header}\n\n{content}\n\n- [ ] {action}"
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

    action = "Investigate failure" if include_action else None
    return _format_message(
        url=url,
        name=name,
        content=failures,
        action=action,
    )


def _format_summary(
    summary: dict,
    include_action: bool = True,
) -> str | None:
    name = summary["dataset_name"]
    url = summary["record_url"]
    if any(
        (not test["success"] and test["required_for_run_success"])
        for test in summary["validation_tests"]
    ):
        return None  # Don't report on file changes if any test failed that was required for the run to succeed.

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
        action = "Review and publish to Zenodo" if include_action else None
    else:
        # If no changes, don't specify an action.
        changes = "No changes."
        action = None

    return _format_message(
        url=url,
        name=name,
        content=changes,
        action=action,
    )


def _format_last_modified(summary: dict) -> str | None:
    """Tabulate how well server metadata predicted which files changed."""
    checks = summary.get("last_modified_checks")
    if not checks:
        return None

    def _mark(value: bool | None) -> str:
        return "?" if value is None else ("yes" if value else "no")

    table = pd.DataFrame.from_records(
        [
            {
                "name": c["name"],
                "last_modified": c["last_modified"] or "",
                "compared_with": c["reference"]
                + (f" ({c['reference_time']})" if c["reference_time"] else ""),
                "last_modified_changed": _mark(c.get("last_modified_changed")),
                "size_differs": _mark(c.get("size_differs")),
                "predicted_changed": _mark(c["predicted_changed"]),
                "actually_changed": _mark(c["actually_changed"]),
                "correct": _mark(
                    None
                    if c["predicted_changed"] is None
                    else c["predicted_changed"] == c["actually_changed"]
                ),
            }
            for c in checks
        ]
    )
    predicted = [c for c in checks if c["predicted_changed"] is not None]
    false_negatives = [
        c["name"]
        for c in predicted
        if c["actually_changed"] and not c["predicted_changed"]
    ]
    false_positives = [
        c["name"]
        for c in predicted
        if c["predicted_changed"] and not c["actually_changed"]
    ]
    correct = len(predicted) - len(false_negatives) - len(false_positives)
    tally = (
        f"{correct}/{len(predicted)} predictions correct, "
        f"{len(false_negatives)} false negatives (predicted unchanged, but changed), "
        f"{len(false_positives)} false positives (predicted changed, but unchanged). "
        f"{len(checks) - len(predicted)} files could not be predicted."
    )
    lm_predicted = [c for c in checks if c.get("last_modified_changed") is not None]
    lm_missed = [
        c["name"]
        for c in lm_predicted
        if c["actually_changed"] and not c["last_modified_changed"]
    ]
    tally += (
        f"\n\nChanged files missed by `Last-Modified` alone (before the size check): "
        f"{len(lm_missed)}" + (f" ({', '.join(lm_missed)})" if lm_missed else "")
    )
    if false_negatives:
        tally += f"\n\n**False negatives:** {', '.join(false_negatives)}"
    return _format_message(
        url=summary["record_url"],
        name=summary["dataset_name"],
        content=f"{tally}\n\n{table.to_markdown(index=False)}",
    )


def _format_metadata_summary(
    summary: dict,
    include_action: bool = True,
) -> str:
    """Describe a Zenodo draft holding only the metadata of a dataset."""
    content = (
        f"New Zenodo metadata draft, version {summary['version']}, "
        f"which will have the DOI {summary['doi']}."
    )
    action = "Review and publish Zenodo metadata draft" if include_action else None
    return _format_message(
        url=summary["record_url"],
        name=f"{summary['dataset_name']} (Zenodo metadata)",
        content=content,
        action=action,
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

    action = "Investigate error" if include_action else None
    return _format_message(
        url=url,
        name=name,
        content=failure,
        action=action,
    )


def _build_markdown_report(
    error_blocks: str,
    failed_blocks: str,
    changed_blocks: str,
    unchanged_blocks: str,
    run_url: str | None = None,
    title: str | None = None,
) -> str:
    """Build a single Markdown report from the formatted summary blocks."""
    parts: list[str] = []

    if title:
        parts.append(title)

    if run_url:
        parts.append(f"[View workflow run]({run_url})")

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
        parts.append("*No downloaded summary or error artifacts were found.*")

    return "\n\n".join(parts)


def _load_summaries(summary_files: tuple[Path, ...]) -> list[dict]:
    summaries = []
    for summary_file in summary_files:
        if summary_file.exists():  # Handle case where no files are found
            with summary_file.open() as f:
                summaries.append(json.loads(f.read()))
    return summaries


def _load_errors(error_files: tuple[Path, ...]) -> list[str]:
    errors = []
    for error_file in error_files:
        if error_file.exists():  # Handle case where no files are found or file is empty
            with error_file.open() as f:
                errors.append(f.read())
    return errors


@click.command()
@click.argument(
    "summary_files",
    nargs=-1,
    required=True,
    type=click.Path(path_type=Path),
)
@click.option(
    "--error-file",
    "error_files",
    multiple=True,
    type=click.Path(path_type=Path),
    help="Log of an archiver run (<dataset>_log.txt), from which the exception of "
    "the run is reported if it crashed. Repeat to give several. Optional, as a run "
    "that succeeded has no error log.",
)
@click.option(
    "--summary-type",
    required=True,
    type=click.Choice(list(SUMMARY_TYPES)),
    help="Which Markdown to output. "
    + " ".join(f"{name}: {text}" for name, text in SUMMARY_TYPES.items()),
)
@click.option(
    "--run-url",
    default=None,
    help="URL of the GitHub Actions workflow run, to link to from the report for "
    "the 'zulip' summary type.",
)
def main(
    summary_files: tuple[Path, ...],
    error_files: tuple[Path, ...],
    summary_type: str,
    run_url: str | None,
) -> None:
    """Format archiver run summaries and error logs as Markdown.

    SUMMARY_FILES are the JSON summaries written by archiver runs
    (<dataset>_run_summary.json, or <dataset>_metadata_summary.json for Zenodo
    metadata drafts). At least one is required, as every run is expected to write
    one. Paths that don't exist are skipped, so a shell glob that matched no files
    is harmless.
    """
    all_summaries = _load_summaries(summary_files)
    # Metadata-only summaries describe a Zenodo draft, not a data archive run.
    metadata_summaries = [s for s in all_summaries if s.get("metadata_only")]
    summaries = [s for s in all_summaries if not s.get("metadata_only")]
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
            itertools.chain(
                (
                    _format_summary(s, include_action)
                    for s in summaries
                    if s["file_changes"]
                ),
                (
                    _format_metadata_summary(s, include_action)
                    for s in metadata_summaries
                ),
            ),
        )
    )

    last_modified_blocks = "\n\n".join(
        filter(None, (_format_last_modified(s) for s in summaries))
    )

    if summary_type == "last_modified":
        click.echo(last_modified_blocks)
    elif summary_type == "change":
        click.echo(changed_blocks)
    elif summary_type == "error":
        click.echo(error_blocks)
    elif summary_type == "failure":
        click.echo(failed_blocks)
    elif summary_type == "unchanged":
        click.echo(unchanged_blocks)
    elif summary_type == "zulip":
        click.echo(
            _build_markdown_report(
                error_blocks=error_blocks,
                failed_blocks=failed_blocks,
                changed_blocks=changed_blocks,
                unchanged_blocks=unchanged_blocks,
                run_url=run_url,
                title="# PUDL data archive run complete.",
            )
        )
    else:
        raise ValueError(f"Unknown summary type: {summary_type!r}")


if __name__ == "__main__":
    main()

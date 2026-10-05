"""Test the script that formats archiver run summaries for GitHub and Zulip."""

import json

from click.testing import CliRunner

from pudl_archiver.scripts.make_markdown_notification import main


def test_summary_type_without_error_files(tmp_path):
    """The workflows ask for some sections without any error files."""
    summary = tmp_path / "ferceqr_run_summary.json"
    summary.write_text(
        json.dumps(
            {
                "dataset_name": "ferceqr",
                "record_url": "gs://x",
                "file_changes": [],
                "validation_tests": [],
            }
        )
    )

    result = CliRunner().invoke(main, [str(summary), "--summary-type", "change"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == ""


def test_error_logs_are_reported_when_the_runs_wrote_no_summary(tmp_path):
    """A run that crashed has an error log but no summary, so the glob matches nothing."""
    logs = []
    for name in ("a", "b"):
        log = tmp_path / f"{name}_log.txt"
        log.write_text(
            f"Traceback (most recent call last):\nValueError: {name} broke\n"
        )
        logs.append(log)

    result = CliRunner().invoke(
        main,
        [
            str(tmp_path / "*_run_summary.json"),
            *(arg for log in logs for arg in ("--error-file", str(log))),
            "--summary-type",
            "error",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "ValueError: a broke" in result.output
    assert "ValueError: b broke" in result.output


def test_summary_files_are_required():
    result = CliRunner().invoke(main, ["--summary-type", "error"])

    assert result.exit_code == 2
    assert "SUMMARY_FILES" in result.output

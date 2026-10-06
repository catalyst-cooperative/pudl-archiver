"""Test the script that formats archiver run summaries for GitHub and Zulip."""

import json

from click.testing import CliRunner

from pudl_archiver.scripts.make_markdown_notification import (
    _format_last_modified,
    main,
)


def test_last_modified_section():
    def check(name, predicted, actual):
        return {
            "name": name,
            "last_modified": "2026-10-03T07:00:00Z",
            "reference": "previous_upload_time",
            "reference_time": "2026-10-02T00:00:00Z",
            "etag": '"abc:0"',
            "content_length": 10,
            "predicted_changed": predicted,
            "actually_changed": actual,
        }

    summary = {
        "dataset_name": "ferceqr",
        "record_url": "gs://x",
        "last_modified_checks": [
            check("a.zip", True, True),
            check("b.zip", None, False),
            check("c.zip", False, True),
        ],
    }

    text = _format_last_modified(summary)

    assert "1/2 predictions correct, 1 false negatives" in text
    assert "1 files could not be predicted" in text
    assert "**False negatives:** c.zip" in text
    assert '"abc:0"' in text
    assert _format_last_modified({"dataset_name": "x", "record_url": "u"}) is None


def test_last_modified_section_without_error_files(tmp_path):
    """The workflow asks for this section without any error files."""
    summary = tmp_path / "ferceqr_run_summary.json"
    summary.write_text(
        json.dumps(
            {
                "dataset_name": "ferceqr",
                "record_url": "gs://x",
                "file_changes": [],
                "validation_tests": [],
                "last_modified_checks": [],
            }
        )
    )

    result = CliRunner().invoke(main, [str(summary), "--summary-type", "last_modified"])

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

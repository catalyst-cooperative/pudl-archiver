"""Detect timed-out matrix runs from a GitHub Actions workflow run.

Queries the GitHub API to find all jobs in the current workflow run
that correspond to the archive-run matrix job and have timed out.
Outputs a list of datasets that timed out.
"""

import os

import requests


def get_timed_out_datasets() -> list[str]:
    """Query GitHub API for timed-out archive-run matrix jobs.

    Returns:
        List of dataset names that timed out.
    """
    # Get environment variables
    github_token = os.getenv("GITHUB_TOKEN")
    github_repository = os.getenv("GITHUB_REPOSITORY")
    github_run_id = os.getenv("GITHUB_RUN_ID")

    if not all([github_token, github_repository, github_run_id]):
        raise AssertionError("Error: Missing required environment variables.")

    # Query the GitHub API for jobs in this workflow run
    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{github_run_id}/jobs?per_page=100"

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }

    run_status = requests.get(url, headers=headers, timeout=1000)
    data = run_status.json()

    jobs = data.get("jobs", [])
    print(jobs)

    # Extract dataset names from timed-out archive-run jobs
    timed_out_datasets = []
    for job in jobs:
        job_name = job.get("name", "")
        print(job_name)
        conclusion = job.get("conclusion")
        print(conclusion)

        # Match jobs that start with "archive-run" and timed out
        if (
            job_name.startswith("archive-run")
            and conclusion == "timed_out"
            and "dataset:" in job_name
        ):
            # Extract dataset name from job name format:  "archive-run (dataset:  dataset_name)"
            dataset = job_name.split("dataset:")[-1].strip().rstrip(")")
            timed_out_datasets.append(dataset)

    return sorted(timed_out_datasets)


def format_timeout_text(timed_out_datasets: list[str]) -> str:
    """Format the timed out datasets into a list."""
    if timed_out_datasets:
        print(
            "<br/><br/><ul>"
            + "</li>".join([f"<li>{dataset}" for dataset in timed_out_datasets])
            + "</li></ul>"
        )
    return None


if __name__ == "__main__":
    timed_out = get_timed_out_datasets()
    format_timeout_text(timed_out)

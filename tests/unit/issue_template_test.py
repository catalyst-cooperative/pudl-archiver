"""Test the GitHub issue template that the archiver workflows fill in."""

from pathlib import Path

import pytest

TEMPLATE = (
    Path(__file__).parents[2]
    / ".github"
    / "ISSUE_TEMPLATE"
    / "monthly-archive-update.md"
)


@pytest.mark.parametrize(
    "run_type,has_fsspec_sections", [("fsspec", True), ("pudl", False), (None, False)]
)
def test_fsspec_sections_depend_on_the_run_type(run_type, has_fsspec_sections):
    """The workflows set RUN_TYPE as an environment variable of the issue action.

    The action's templates only see environment variables as `env.NAME`, and a bare
    name is silently undefined, which once left the fsspec sections out of every issue.
    """
    jinja2 = pytest.importorskip(
        "jinja2"
    )  # Nunjucks, which the action uses, is similar
    env = {"RUN_TYPE": run_type} if run_type else {}

    issue = jinja2.Template(TEMPLATE.read_text()).render(env=env)

    assert ("Publishing fsspec archives" in issue) is has_fsspec_sections

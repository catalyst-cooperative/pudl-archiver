"""Test what a failed run leaves in the draft."""

import pytest

from pudl_archiver import archive_dataset

V1 = {"a.txt": b"a contents", "b.txt": b"b contents"}
# The first file has changed, but then the archiver can't get the second
V2 = {"a.txt": b"a, now changed", "b.txt": b"b contents"}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", ["finding", "downloading"])
async def test_failure_to_download_leaves_the_draft_and_archive_as_they_were(
    fake_archive, fail
):
    """E.g. the data provider dropped a file from its website, so the archiver stops."""
    _, settings = fake_archive.prepare(V1, 1, initialize=True)
    await archive_dataset("pudl_test", settings)
    published = fake_archive.published()
    assert set(published) == {"a.txt", "b.txt", "datapackage.json"}

    summary_file, settings = fake_archive.prepare(V2, 2, fail=fail)
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)

    # Nothing that was archived has changed
    assert fake_archive.published() == published
    # The draft has only what was uploaded, and no datapackage.json that says the
    # archive is smaller than it is
    assert {p.name for p in fake_archive.workspace.glob("*")} == (
        {"a.txt"} if fail == "downloading" else set()
    )
    # And the run reports the failure itself, not failures that follow from it
    summary = fake_archive.load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]
    assert summary.file_changes == []

"""Test what a failed run leaves in the draft, and resuming it with a retry."""

import pytest

from pudl_archiver import archive_dataset
from pudl_archiver.depositors import get_deposition
from pudl_archiver.depositors.depositor import (
    INCOMPLETE_MARKER,
    IncompleteDepositionError,
)
from pudl_archiver.frictionless import DataPackage

V1 = {"a.txt": b"a contents", "b.txt": b"b contents"}
# The first file has changed, but then the archiver can't get the second
V2 = {"a.txt": b"a, now changed", "b.txt": b"b contents"}


async def _archive_v1_then_fail_v2(fake_archive, fail="downloading"):
    """Archive V1, then fail to archive V2, returning the summary of the failure."""
    _, settings = fake_archive.prepare(V1, 1, initialize=True)
    await archive_dataset("pudl_test", settings)
    summary_file, settings = fake_archive.prepare(V2, 2, fail=fail)
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)
    return fake_archive.load(summary_file)


async def _retry(fake_archive, failed, auto_publish=True, trust_all=False):
    """Retry a failed run the way `retry-run` does, returning its summary."""
    summary_file, settings = fake_archive.prepare(V2, 3, auto_publish=auto_publish)
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=failed.successful_partitions,
        skip_checksums={} if trust_all else failed.uploaded_checksums,
    )
    return fake_archive.load(summary_file)


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
    # The draft has only what was uploaded, no datapackage.json that says the
    # archive is smaller than it is, and a marker that it isn't to be published
    assert {p.name for p in fake_archive.workspace.glob("*")} == {
        INCOMPLETE_MARKER,
        *({"a.txt"} if fail == "downloading" else set()),
    }
    marker = (fake_archive.workspace / INCOMPLETE_MARKER).read_text()
    assert "must not be published" in marker
    assert "RuntimeError: " in marker
    # And the run reports the failure itself, not failures that follow from it
    summary = fake_archive.load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]
    assert summary.file_changes == []
    assert set(summary.uploaded_checksums) == (
        {"a.txt"} if fail == "downloading" else set()
    )


@pytest.mark.asyncio
async def test_retry_resumes_from_a_failed_draft(fake_archive):
    """The files that the failed run uploaded stay, and aren't downloaded again."""
    failed = await _archive_v1_then_fail_v2(fake_archive)
    fake_archive.downloaded.clear()

    summary = await _retry(fake_archive, failed)

    assert summary.success
    assert fake_archive.downloaded == ["b.txt"]
    # The archive is as it would be if the first attempt hadn't failed
    published = fake_archive.published()
    assert set(published) == {"a.txt", "b.txt", "datapackage.json"}
    assert published["a.txt"] == b"a, now changed"
    datapackage = DataPackage.model_validate_json(published["datapackage.json"])
    assert {r.name for r in datapackage.resources} == {"a.txt", "b.txt"}
    assert [c.name for c in summary.file_changes] == ["a.txt"]
    # and the draft is gone, with its marker
    assert not list(fake_archive.workspace.glob("*"))
    # The retry says what it uploaded, so that another retry could skip it too
    assert set(summary.uploaded_checksums) == {"a.txt", "b.txt"}


@pytest.mark.asyncio
async def test_retry_that_is_not_published_is_no_longer_marked_incomplete(
    fake_archive,
):
    """A retry that doesn't publish leaves a draft that `publish-run` can publish."""
    failed = await _archive_v1_then_fail_v2(fake_archive)
    before = fake_archive.published()

    retried = await _retry(fake_archive, failed, auto_publish=False)

    assert retried.success
    # b.txt hasn't changed, so it is only in the published version
    assert {p.name for p in fake_archive.workspace.glob("*")} == {
        "a.txt",
        "datapackage.json",
    }
    assert fake_archive.published() == before

    # `publish-run`: archive again, skipping everything the retry archived
    fake_archive.downloaded.clear()
    summary_file, settings = fake_archive.prepare(V2, 4)
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=retried.successful_partitions,
        skip_checksums=retried.uploaded_checksums,
    )
    assert fake_archive.load(summary_file).success
    assert fake_archive.downloaded == []
    assert fake_archive.published()["a.txt"] == b"a, now changed"
    assert not list(fake_archive.workspace.glob("*"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "edit,trust_all,expected",
    [
        ("edit", False, ["a.txt", "b.txt"]),
        ("delete", False, ["a.txt", "b.txt"]),
        # Run summaries from before the checksums were recorded don't have them
        ("edit", True, ["b.txt"]),
    ],
    ids=["edited", "deleted", "unverifiable"],
)
async def test_retry_only_skips_files_that_are_in_the_draft_as_they_were(
    fake_archive, edit, trust_all, expected
):
    failed = await _archive_v1_then_fail_v2(fake_archive)
    uploaded = fake_archive.workspace / "a.txt"
    if edit == "edit":
        uploaded.write_bytes(b"tampered with")
    else:
        uploaded.unlink()
    fake_archive.downloaded.clear()

    summary = await _retry(fake_archive, failed, trust_all=trust_all)

    assert summary.success
    assert fake_archive.downloaded == expected


@pytest.mark.asyncio
async def test_a_draft_marked_incomplete_cannot_be_published(fake_archive, mocker):
    await _archive_v1_then_fail_v2(fake_archive)
    before = fake_archive.published()
    _, settings = fake_archive.prepare(V2, 3)
    draft, _ = await get_deposition("pudl_test", mocker.MagicMock(), settings)

    with pytest.raises(IncompleteDepositionError, match=INCOMPLETE_MARKER):
        await draft.publish()

    assert fake_archive.published() == before


@pytest.mark.asyncio
async def test_first_run_that_fails_is_marked_incomplete_too(fake_archive):
    """Without a previous version, there is no datapackage.json to leave either."""
    summary_file, settings = fake_archive.prepare(
        V1, 1, initialize=True, fail="downloading"
    )
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)

    assert {p.name for p in fake_archive.workspace.glob("*")} == {
        "a.txt",
        INCOMPLETE_MARKER,
    }
    assert not (fake_archive.deposition / "published").exists()
    summary = fake_archive.load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]


@pytest.mark.asyncio
async def test_retry_of_a_first_run_that_fails_again_is_still_marked_incomplete(
    fake_archive,
):
    """The marker of the failed run isn't a file of the archive that is described."""
    _, settings = fake_archive.prepare(V1, 1, initialize=True, fail="downloading")
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)
    assert (fake_archive.workspace / INCOMPLETE_MARKER).exists()

    summary_file, settings = fake_archive.prepare(
        V1, 2, initialize=True, fail="downloading"
    )
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)

    assert {p.name for p in fake_archive.workspace.glob("*")} == {
        "a.txt",
        INCOMPLETE_MARKER,
    }
    assert [t.name for t in fake_archive.load(summary_file).get_failed_tests()] == [
        "Run exception validation test"
    ]

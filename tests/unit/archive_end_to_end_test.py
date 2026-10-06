"""Run whole archives of a fake dataset to the local filesystem, without a network.

A real FERC EQR run takes hours before it gets to write out the run summary and
datapackage, so these tests exercise everything after the downloads: validation,
datapackage generation, and the run summary file.
"""

import json

import pytest

from pudl_archiver import archive_dataset
from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.depositors import get_deposition
from pudl_archiver.depositors.depositor import (
    INCOMPLETE_MARKER,
    IncompleteDepositionError,
)
from pudl_archiver.frictionless import DataPackage, ResourceInfo
from pudl_archiver.utils import RunSettings

pytestmark = pytest.mark.usefixtures("fake_dataset")


downloaded = []  # The files that the archivers downloaded


def _run_archive(
    mocker, tmp_path, files, run_number, initialize=False, fail=None, auto_publish=True
):
    """Archive ``files`` (name -> contents).

    ``fail`` makes the archiver fail "finding" the files, before downloading any of
    them, or "downloading" the last one.
    """

    class FakeArchiver(AbstractDatasetArchiver):
        name = "pudl_test"
        concurrency_limit = 1  # Download one at a time, in order
        fail_on_file_size_change = False
        fail_on_dataset_size_change = False
        fail_on_missing_files = False
        fail_on_data_continuity = False

        async def get_resources(self):
            if fail == "finding":
                raise RuntimeError("b.txt has gone from the website")
            for filename, contents in files.items():
                partitions = {"name": filename}
                yield (
                    self.get_file(filename, contents, partitions),
                    partitions,
                )

        async def get_file(self, filename, contents, partitions):
            if fail == "downloading" and filename == list(files)[-1]:
                raise RuntimeError("the download of b.txt failed")
            downloaded.append(filename)
            path = self.download_directory / filename
            path.write_bytes(contents)
            return ResourceInfo(local_path=path, partitions=partitions)

    mocker.patch.dict("pudl_archiver.ARCHIVERS", {"pudl_test": FakeArchiver})
    summary_file = tmp_path / f"run{run_number}_summary.json"
    return summary_file, RunSettings(
        clobber_unchanged=True,
        auto_publish=auto_publish,
        initialize=initialize,
        depositor="fsspec",
        depositor_args={"deposition_path": str(tmp_path / "deposition")},
        summary_file=str(summary_file),
    )


def _load(summary_file) -> RunSummary:
    # What the notification scripts and `retry-run`/`publish-run` do with the file
    return RunSummary.model_validate(json.loads(summary_file.read_text()))


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", ["finding", "downloading"])
async def test_failure_to_download_leaves_the_draft_and_archive_as_they_were(
    mocker, tmp_path, fail
):
    """E.g. the data provider dropped a file from its website, so the archiver stops."""
    (tmp_path / "deposition").mkdir()
    v1 = {
        "a.txt": b"a contents",
        "b.txt": b"b contents",
    }
    summary_file, settings = _run_archive(mocker, tmp_path, v1, 1, initialize=True)
    await archive_dataset("pudl_test", settings)
    deposition = tmp_path / "deposition"
    published = {p.name: p.read_bytes() for p in (deposition / "published").iterdir()}
    assert set(published) == {"a.txt", "b.txt", "datapackage.json"}

    # The first file has changed, but then the archiver can't get the second
    v2 = {
        "a.txt": b"a, now changed",
        "b.txt": b"b contents",
    }
    summary_file, settings = _run_archive(mocker, tmp_path, v2, 2, fail=fail)
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)

    # Nothing that was archived has changed
    after = {p.name: p.read_bytes() for p in (deposition / "published").iterdir()}
    assert after == published
    # The draft has only what was uploaded, no datapackage.json that says the
    # archive is smaller than it is, and a marker that it isn't to be published
    assert {p.name for p in (deposition / "workspace").glob("*")} == {
        INCOMPLETE_MARKER,
        *({"a.txt"} if fail == "downloading" else set()),
    }
    marker = (deposition / "workspace" / INCOMPLETE_MARKER).read_text()
    assert "must not be published" in marker
    assert "RuntimeError: " in marker
    # And the run reports the failure itself, not failures that follow from it
    summary = _load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]
    assert summary.file_changes == []
    assert set(summary.uploaded_checksums) == (
        {"a.txt"} if fail == "downloading" else set()
    )


V1 = {
    "a.txt": b"a contents",
    "b.txt": b"b contents",
}
# The first file has changed, but then the archiver can't get the second
V2 = {
    "a.txt": b"a, now changed",
    "b.txt": b"b contents",
}


async def _archive_v1_then_fail_v2(mocker, tmp_path, fail="downloading"):
    """Archive V1, then fail to archive V2, returning the summary of the failure."""
    (tmp_path / "deposition").mkdir()
    _, settings = _run_archive(mocker, tmp_path, V1, 1, initialize=True)
    await archive_dataset("pudl_test", settings)
    summary_file, settings = _run_archive(mocker, tmp_path, V2, 2, fail=fail)
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)
    return _load(summary_file)


async def _retry(mocker, tmp_path, failed, auto_publish=True, trust_all=False):
    """Retry a failed run the way `retry-run` does, returning its summary."""
    summary_file, settings = _run_archive(
        mocker, tmp_path, V2, 3, auto_publish=auto_publish
    )
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=failed.successful_partitions,
        skip_checksums={} if trust_all else failed.uploaded_checksums,
    )
    return _load(summary_file)


def _published(tmp_path) -> dict[str, bytes]:
    return {
        p.name: p.read_bytes()
        for p in (tmp_path / "deposition" / "published").iterdir()
    }


@pytest.mark.asyncio
async def test_retry_resumes_from_a_failed_draft(mocker, tmp_path):
    """The files that the failed run uploaded stay, and aren't downloaded again."""
    failed = await _archive_v1_then_fail_v2(mocker, tmp_path)
    downloaded.clear()

    summary = await _retry(mocker, tmp_path, failed)

    assert summary.success
    assert downloaded == ["b.txt"]
    # The archive is as it would be if the first attempt hadn't failed
    published = _published(tmp_path)
    assert set(published) == {"a.txt", "b.txt", "datapackage.json"}
    assert published["a.txt"] == b"a, now changed"
    datapackage = DataPackage.model_validate_json(published["datapackage.json"])
    assert {r.name for r in datapackage.resources} == {"a.txt", "b.txt"}
    assert [c.name for c in summary.file_changes] == ["a.txt"]
    # and the draft is gone, with its marker
    assert not list((tmp_path / "deposition" / "workspace").glob("*"))
    # The retry says what it uploaded, so that another retry could skip it too
    assert set(summary.uploaded_checksums) == {"a.txt", "b.txt"}


@pytest.mark.asyncio
async def test_retry_that_is_not_published_is_no_longer_marked_incomplete(
    mocker, tmp_path
):
    """A retry that doesn't publish leaves a draft that `publish-run` can publish."""
    failed = await _archive_v1_then_fail_v2(mocker, tmp_path)
    before = _published(tmp_path)

    retried = await _retry(mocker, tmp_path, failed, auto_publish=False)

    assert retried.success
    workspace = tmp_path / "deposition" / "workspace"
    # b.txt hasn't changed, so it is only in the published version
    assert {p.name for p in workspace.glob("*")} == {"a.txt", "datapackage.json"}
    assert _published(tmp_path) == before

    # `publish-run`: archive again, skipping everything the retry archived
    downloaded.clear()
    summary_file, settings = _run_archive(mocker, tmp_path, V2, 4)
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=retried.successful_partitions,
        skip_checksums=retried.uploaded_checksums,
    )
    assert _load(summary_file).success
    assert downloaded == []
    assert _published(tmp_path)["a.txt"] == b"a, now changed"
    assert not list(workspace.glob("*"))


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
    mocker, tmp_path, edit, trust_all, expected
):
    failed = await _archive_v1_then_fail_v2(mocker, tmp_path)
    uploaded = tmp_path / "deposition" / "workspace" / "a.txt"
    if edit == "edit":
        uploaded.write_bytes(b"tampered with")
    else:
        uploaded.unlink()
    downloaded.clear()

    summary = await _retry(mocker, tmp_path, failed, trust_all=trust_all)

    assert summary.success
    assert downloaded == expected


@pytest.mark.asyncio
async def test_a_draft_marked_incomplete_cannot_be_published(mocker, tmp_path):
    await _archive_v1_then_fail_v2(mocker, tmp_path)
    before = _published(tmp_path)
    _, settings = _run_archive(mocker, tmp_path, V2, 3)
    draft, _ = await get_deposition("pudl_test", mocker.MagicMock(), settings)

    with pytest.raises(IncompleteDepositionError, match=INCOMPLETE_MARKER):
        await draft.publish()

    assert _published(tmp_path) == before


@pytest.mark.asyncio
async def test_first_run_that_fails_is_marked_incomplete_too(mocker, tmp_path):
    """Without a previous version, there is no datapackage.json to leave either."""
    (tmp_path / "deposition").mkdir()
    summary_file, settings = _run_archive(
        mocker, tmp_path, V1, 1, initialize=True, fail="downloading"
    )
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)

    workspace = tmp_path / "deposition" / "workspace"
    assert {p.name for p in workspace.glob("*")} == {"a.txt", INCOMPLETE_MARKER}
    assert not (tmp_path / "deposition" / "published").exists()
    summary = _load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]

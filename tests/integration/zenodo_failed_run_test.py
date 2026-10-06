"""Test what a failed run leaves in a draft on the Zenodo sandbox, and resuming it."""

import asyncio
import hashlib
import io

import pytest

from pudl_archiver.archivers.classes import AbstractDatasetArchiver, ResourceInfo
from pudl_archiver.depositors import get_deposition
from pudl_archiver.depositors.depositor import (
    INCOMPLETE_MARKER,
    IncompleteDepositionError,
)
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings

V1 = {
    "a.txt": b"a, as first archived",
    "b.txt": b"b, which never changes",
    "c.txt": b"c, as first archived",
}
V2 = {
    "a.txt": b"a, changed",
    "b.txt": V1["b.txt"],
    "c.txt": b"c, changed",
}


def md5(contents: bytes) -> str:
    """Zenodo's checksum of a file."""
    return hashlib.md5(contents, usedforsecurity=False).hexdigest()


class FakeDownloader(AbstractDatasetArchiver):
    """Download the files in order, failing on one of them, if it is told to."""

    name = "pudl_test"
    concurrency_limit = None  # Download everything at once, like most archivers
    fail_on_file_size_change = False
    fail_on_dataset_size_change = False
    fail_on_missing_files = False
    fail_on_data_continuity = False

    def __init__(self, contents, failing=None, **kwargs):
        super().__init__(**kwargs)
        self.contents = contents
        self.failing = failing
        self.downloaded = []

    async def get_resources(self):
        for filename in self.contents:
            partitions = {"name": filename}
            yield self.get_file(filename, partitions), partitions

    async def get_file(self, filename, partitions):
        if filename == self.failing:
            await asyncio.sleep(0.01)  # The other files finish first
            raise RuntimeError(f"{filename} has gone from the website")
        self.downloaded.append(filename)
        path = self.download_directory / filename
        path.write_bytes(self.contents[filename])
        return ResourceInfo(local_path=path, partitions=partitions)


@pytest.mark.asyncio
async def test_failed_run_leaves_a_marked_draft_that_a_retry_resumes(
    session, zenodo_sandbox, mocker
):
    """Fail in the middle of an update, then resume from the draft that is left."""
    settings = RunSettings(
        clobber_unchanged=True,
        auto_publish=True,
        initialize=True,
        depositor="zenodo",
        depositor_args={"sandbox": True},
    )
    v1_summary, _ = await orchestrate_run(
        dataset="pudl_test",
        downloader=FakeDownloader(V1, session=session),
        run_settings=settings,
        session=session,
    )
    assert v1_summary.success
    settings = settings.model_copy(update={"initialize": False})

    # Update all but b.txt, but then the archiver can't get c.txt
    mocker.patch("pudl_archiver.depositors.depositor.logger.error")
    mocker.patch("pudl_archiver.orchestrator.logger.exception")
    failed, _ = await orchestrate_run(
        dataset="pudl_test",
        downloader=FakeDownloader(V2, failing="c.txt", session=session),
        run_settings=settings,
        session=session,
    )
    await asyncio.sleep(1)

    assert not failed.success
    assert [t.name for t in failed.get_failed_tests()] == [
        "Run exception validation test"
    ]
    assert failed.file_changes == []
    # b.txt was downloaded and found unchanged, and a.txt was uploaded
    assert set(failed.uploaded_checksums) == {"a.txt", "b.txt"}
    assert failed.uploaded_checksums["a.txt"] == md5(V2["a.txt"])

    # A new version of a Zenodo record starts as an empty draft, so the draft only has
    # what the failed run uploaded. It has no datapackage.json that describes it,
    # and says why.
    draft, _ = await get_deposition("pudl_test", session, settings)
    assert set(await draft.list_files()) == {"a.txt", "b.txt", INCOMPLETE_MARKER}
    assert draft.get_checksum("a.txt") == md5(V2["a.txt"])
    assert draft.get_checksum("b.txt") == md5(V2["b.txt"])
    # `get_file` can only fetch published files, so use the draft file's own link
    marker_link = draft.deposition.files_map[INCOMPLETE_MARKER].links.download
    async with session.get(str(marker_link), headers=draft.api_client.auth_write) as r:
        assert r.status == 200
        marker = await r.text()
    assert "must not be published" in marker
    assert "c.txt has gone from the website" in marker
    # and it can't be published
    with pytest.raises(IncompleteDepositionError):
        await draft.publish()

    # Someone edits a file in the draft, before the retry
    draft = await draft.delete_file("b.txt")
    draft = await draft.create_file("b.txt", io.BytesIO(b"edited by hand"))
    await asyncio.sleep(1)

    # Retry: only skip what is still in the draft as the failed run left it
    retry = FakeDownloader(V2, session=session)
    retried, published = await orchestrate_run(
        dataset="pudl_test",
        downloader=retry,
        run_settings=settings,
        session=session,
        skip_partitions=failed.successful_partitions,
        skip_checksums=failed.uploaded_checksums,
    )

    assert retried.success
    # a.txt was uploaded by the failed run, and is still there. b.txt isn't.
    assert sorted(retry.downloaded) == ["b.txt", "c.txt"]
    assert INCOMPLETE_MARKER not in published.deposition.files_map
    datapackage = DataPackage.model_validate_json(
        await published.get_file("datapackage.json")
    )
    assert {r.name: r.hash_ for r in datapackage.resources} == {
        name: md5(contents) for name, contents in V2.items()
    }
    assert [c.name for c in retried.file_changes] == ["a.txt", "c.txt"]

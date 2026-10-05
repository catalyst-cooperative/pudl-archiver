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
from pudl_archiver.frictionless import ResourceInfo
from pudl_archiver.metadata.constants import LICENSES
from pudl_archiver.utils import RunSettings


@pytest.fixture(autouse=True)
def fake_dataset(mocker):
    """Make 'pudl_test' a dataset with a fake archiver."""
    mocker.patch(
        "pudl_archiver.frictionless.get_pudl_sources",
        return_value={
            "pudl_test": {
                "name": "pudl_test",
                "title": "Pudl Test",
                "description": "Test dataset",
                "path": "https://fake.link",
                "license_raw": LICENSES["cc-by-4.0"],
                "contributors": [
                    {
                        "title": "Catalyst Cooperative",
                        "organization": "Catalyst Cooperative",
                    }
                ],
            }
        },
    )


def _run_archive(mocker, tmp_path, files, run_number, initialize=False, fail=None):
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
            path = self.download_directory / filename
            path.write_bytes(contents)
            return ResourceInfo(local_path=path, partitions=partitions)

    mocker.patch.dict("pudl_archiver.ARCHIVERS", {"pudl_test": FakeArchiver})
    summary_file = tmp_path / f"run{run_number}_summary.json"
    return summary_file, RunSettings(
        clobber_unchanged=True,
        auto_publish=True,
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
    # The draft has only what was uploaded, and no datapackage.json that says the
    # archive is smaller than it is
    assert {p.name for p in (deposition / "workspace").glob("*")} == (
        {"a.txt"} if fail == "downloading" else set()
    )
    # And the run reports the failure itself, not failures that follow from it
    summary = _load(summary_file)
    assert [t.name for t in summary.get_failed_tests()] == [
        "Run exception validation test"
    ]
    assert summary.file_changes == []

"""Unit test configuration."""

import json
import os
from pathlib import Path

import pytest

# Use the checked-in fixture descriptor so unit tests never fetch from S3.
os.environ.setdefault(
    "PUDL_DATAPACKAGE_PATH",
    str(Path(__file__).parent.parent / "fixtures" / "datapackage.json"),
)

from pudl_archiver.archivers.classes import AbstractDatasetArchiver
from pudl_archiver.archivers.validate import RunSummary
from pudl_archiver.frictionless import ResourceInfo
from pudl_archiver.metadata.constants import LICENSES
from pudl_archiver.utils import RunSettings


@pytest.fixture
def fake_dataset(mocker):
    """Make 'pudl_test' a dataset that can be archived, without a real data source."""
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


class FakeArchive:
    """Archive whole fake datasets to a local fsspec deposition, without a network.

    A real FERC EQR run takes hours before it gets to write out the run summary and
    datapackage, so tests of what happens after the downloads (validation, the
    datapackage, the run summary file, what a failed run leaves in the draft) run
    archives of a dataset with a few small files instead.
    """

    def __init__(self, mocker, tmp_path):
        self._mocker = mocker
        self._tmp_path = tmp_path
        self.deposition = tmp_path / "deposition"
        self.deposition.mkdir()
        #: The files that the archivers downloaded, in order
        self.downloaded: list[str] = []

    @property
    def workspace(self) -> Path:
        """The draft of the deposition."""
        return self.deposition / "workspace"

    def published(self) -> dict[str, bytes]:
        """The contents of the files of the published deposition."""
        return {
            p.name: p.read_bytes() for p in (self.deposition / "published").iterdir()
        }

    @staticmethod
    def load(summary_file) -> RunSummary:
        """Load a run summary as the notification scripts and ``retry-run`` do."""
        return RunSummary.model_validate(json.loads(summary_file.read_text()))

    def prepare(
        self,
        files: dict,
        run_number: int,
        initialize: bool = False,
        fail: str | None = None,
        auto_publish: bool = True,
    ) -> tuple[Path, RunSettings]:
        """Make the next archive run archive ``files``.

        Args:
            files: The contents of each file by name, or a tuple of the contents and
                a dictionary of what to add to the ``ResourceInfo`` of the file.
            run_number: Used to name the summary file of the run.
            initialize: Whether this is the first run, which creates the deposition.
            fail: Make the archiver fail "finding" the files, before downloading any
                of them, or "downloading" the last one.
            auto_publish: Whether to publish the archive if it is valid.

        Returns:
            The path of the summary file that the run writes, and its settings, to
            pass to ``archive_dataset``.
        """
        archive = self

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
                    yield self.get_file(filename, contents, partitions), partitions

            async def get_file(self, filename, contents, partitions):
                if fail == "downloading" and filename == list(files)[-1]:
                    raise RuntimeError("the download of b.txt failed")
                contents, extra = (
                    contents if isinstance(contents, tuple) else (contents, {})
                )
                archive.downloaded.append(filename)
                path = self.download_directory / filename
                path.write_bytes(contents)
                return ResourceInfo(local_path=path, partitions=partitions, **extra)

        self._mocker.patch.dict("pudl_archiver.ARCHIVERS", {"pudl_test": FakeArchiver})
        summary_file = self._tmp_path / f"run{run_number}_summary.json"
        return summary_file, RunSettings(
            clobber_unchanged=True,
            auto_publish=auto_publish,
            initialize=initialize,
            depositor="fsspec",
            depositor_args={"deposition_path": str(self.deposition)},
            summary_file=str(summary_file),
        )


@pytest.fixture
def fake_archive(mocker, tmp_path, fake_dataset) -> FakeArchive:
    """Archive whole fake datasets to a local deposition."""
    return FakeArchive(mocker, tmp_path)

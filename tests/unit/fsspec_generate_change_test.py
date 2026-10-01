"""Test FsspecDraftDeposition.generate_change file diffing logic."""

from pathlib import Path

from upath import UPath

from pudl_archiver.depositors.depositor import DepositionAction
from pudl_archiver.depositors.fsspec import (
    Deposition,
    FsspecAPIClient,
    FsspecDraftDeposition,
)
from pudl_archiver.frictionless import ResourceInfo
from pudl_archiver.utils import RunSettings


def _draft(
    deposition_path: Path,
    published: dict[str, bytes],
    workspace: dict[str, bytes] | None = None,
) -> FsspecDraftDeposition:
    for directory, files in [
        ("published", published),
        ("workspace", workspace or {}),
    ]:
        (deposition_path / directory).mkdir(parents=True)
        for name, contents in files.items():
            (deposition_path / directory / name).write_bytes(contents)
    return FsspecDraftDeposition(
        dataset_id="ferceqr",
        settings=RunSettings(depositor="fsspec"),
        api_client=FsspecAPIClient(path=str(deposition_path)),
        deposition=Deposition.from_upath(UPath(deposition_path)),
    )


def _resource(tmp_path: Path, contents: bytes) -> ResourceInfo:
    local_path = tmp_path / "resource.zip"
    local_path.write_bytes(contents)
    return ResourceInfo(local_path=local_path, partitions={})


def test_generate_change_new_file_is_create(tmp_path):
    draft = _draft(tmp_path / "deposition", published={})

    change = draft.generate_change("resource.zip", _resource(tmp_path, b"new"))

    assert change.action_type == DepositionAction.CREATE


def test_generate_change_modified_file_is_update(tmp_path):
    draft = _draft(tmp_path / "deposition", published={"resource.zip": b"old"})

    change = draft.generate_change("resource.zip", _resource(tmp_path, b"new"))

    assert change.action_type == DepositionAction.UPDATE


def test_generate_change_unchanged_file_is_no_op(tmp_path):
    draft = _draft(tmp_path / "deposition", published={"resource.zip": b"same"})

    change = draft.generate_change("resource.zip", _resource(tmp_path, b"same"))

    assert change.action_type == DepositionAction.NO_OP


def test_generate_change_file_already_in_workspace_is_no_op(tmp_path):
    """A retry must not re-upload files a previous run already put in the draft."""
    draft = _draft(
        tmp_path / "deposition",
        published={"resource.zip": b"old"},
        workspace={"resource.zip": b"new"},
    )

    change = draft.generate_change("resource.zip", _resource(tmp_path, b"new"))

    assert change.action_type == DepositionAction.NO_OP

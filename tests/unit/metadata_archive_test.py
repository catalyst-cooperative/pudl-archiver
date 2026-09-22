"""Test archiving only the datapackage.json of an fsspec archive on Zenodo."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from upath import UPath

from pudl_archiver import orchestrator
from pudl_archiver.archivers.validate import _datapackage_changed
from pudl_archiver.depositors.fsspec import (
    Deposition,
    FsspecAPIClient,
    FsspecDraftDeposition,
)
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.orchestrator import orchestrate_metadata_archive
from pudl_archiver.utils import RunSettings

DOI_URL = "https://doi.org/10.5281/zenodo.123"


def _datapackage(**updates) -> DataPackage:
    datapackage = DataPackage(
        name="ferceqr",
        title="FERC EQR",
        description="Description",
        keywords=[],
        contributors=[],
        sources=[],
        licenses=[],
        resources=[],
        created="2026-01-01T00:00:00+00:00",
    )
    return datapackage.model_copy(update=updates)


class FakeZenodoDraft:
    """Stand-in for a ZenodoDraftDeposition that records what was done to it."""

    reserved_doi = "10.5281/zenodo.123"

    def __init__(self, files=("datapackage.json",), version="2.0.0"):
        self.deposition = SimpleNamespace(metadata=SimpleNamespace(version=version))
        self.files = list(files)
        self.changes = []
        self.uploaded = None
        self.deleted = False

    async def list_files(self):
        return self.files

    def get_deposition_link(self):
        return "https://zenodo.org/deposit/123"

    async def _apply_change(self, change):
        self.changes.append(change)
        self.uploaded = change.resource.read_bytes()
        return self

    async def delete_deposition(self):
        self.deleted = True


@pytest.fixture()
def workspace(tmp_path) -> Path:
    (tmp_path / "workspace").mkdir()
    return tmp_path / "workspace" / "datapackage.json"


def _patch_get_deposition(monkeypatch, draft, original):
    async def get_deposition(dataset, session, run_settings):
        return draft, original

    monkeypatch.setattr(orchestrator, "get_deposition", get_deposition)


async def _run(tmp_path):
    return await orchestrate_metadata_archive(
        dataset="ferceqr",
        source_path=str(tmp_path),
        run_settings=RunSettings(depositor="zenodo"),
        session=None,
    )


def test_datapackage_id_only_serialized_when_set():
    assert "id" not in json.loads(_datapackage().model_dump_json(by_alias=True))
    with_id = _datapackage(id_=DOI_URL)
    dumped = with_id.model_dump_json(by_alias=True)
    assert json.loads(dumped)["id"] == DOI_URL
    assert DataPackage.model_validate_json(dumped).id_ == DOI_URL


def test_datapackage_changed_ignores_id_version_and_created():
    original = _datapackage(id_=DOI_URL, version="1.0.0")
    new = _datapackage(id_=None, version="2.0.0", created="2027-01-01T00:00:00+00:00")
    assert not _datapackage_changed(original, new)
    assert _datapackage_changed(original, _datapackage(title="Something else"))


@pytest.mark.asyncio
async def test_metadata_archive_stamps_and_uploads(tmp_path, workspace, monkeypatch):
    workspace.write_text(_datapackage(version="0.1").model_dump_json(by_alias=True))
    draft = FakeZenodoDraft()
    original = _datapackage(title="Older title", version="1.0.0")
    _patch_get_deposition(monkeypatch, draft, original)

    summary = await _run(tmp_path)

    assert summary.version == "2.0.0"
    assert str(summary.doi) == DOI_URL
    assert not draft.deleted
    assert len(draft.changes) == 1
    assert draft.changes[0].name == "datapackage.json"
    assert draft.changes[0].action_type.name == "UPDATE"
    # The same stamped file lands on Zenodo and in the fsspec workspace
    stamped = json.loads(draft.uploaded)
    assert stamped["version"] == "2.0.0"
    assert stamped["id"] == DOI_URL
    assert workspace.read_bytes() == draft.uploaded


@pytest.mark.asyncio
async def test_metadata_archive_new_lineage_creates_file(
    tmp_path, workspace, monkeypatch
):
    workspace.write_text(_datapackage().model_dump_json(by_alias=True))
    draft = FakeZenodoDraft(files=[], version="1.0.0")
    _patch_get_deposition(monkeypatch, draft, None)

    await _run(tmp_path)

    assert draft.changes[0].action_type.name == "CREATE"


@pytest.mark.asyncio
async def test_metadata_archive_unchanged_deletes_draft(
    tmp_path, workspace, monkeypatch
):
    workspace.write_text(_datapackage(version="0.1").model_dump_json(by_alias=True))
    draft = FakeZenodoDraft()
    _patch_get_deposition(monkeypatch, draft, _datapackage(version="1.0.0"))
    before = workspace.read_bytes()

    assert await _run(tmp_path) is None

    assert draft.deleted
    assert draft.changes == []
    assert workspace.read_bytes() == before


@pytest.mark.asyncio
async def test_metadata_archive_without_workspace_datapackage(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("Should not touch Zenodo without a new datapackage.")

    monkeypatch.setattr(orchestrator, "get_deposition", fail)

    assert await _run(tmp_path) is None


def test_fsspec_regenerated_datapackage_keeps_stamp(tmp_path, workspace):
    """Publishing a run regenerates the datapackage, which must keep its stamp."""
    workspace.write_text(
        _datapackage(version="2.0.0", id_=DOI_URL).model_dump_json(by_alias=True)
    )
    draft = FsspecDraftDeposition(
        deposition=Deposition.from_upath(UPath(tmp_path)),
        settings=RunSettings(depositor="fsspec"),
        api_client=FsspecAPIClient(path=str(tmp_path)),
        dataset_id="ferceqr",
    )

    regenerated = draft.generate_datapackage({})

    assert regenerated.version == "2.0.0"
    assert regenerated.id_ == DOI_URL


def test_fsspec_new_datapackage_has_no_stamp(tmp_path):
    (tmp_path / "workspace").mkdir()
    draft = FsspecDraftDeposition(
        deposition=Deposition.from_upath(UPath(tmp_path)),
        settings=RunSettings(depositor="fsspec"),
        api_client=FsspecAPIClient(path=str(tmp_path)),
        dataset_id="ferceqr",
    )

    regenerated = draft.generate_datapackage({})

    assert regenerated.version == "0.1"
    assert regenerated.id_ is None

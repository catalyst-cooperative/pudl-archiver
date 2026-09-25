"""Test ZenodoDraftDeposition.generate_change file diffing logic."""

import datetime
from hashlib import md5
from pathlib import Path

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.depositors.depositor import DepositionAction
from pudl_archiver.depositors.zenodo.depositor import (
    ZenodoAPIClient,
    ZenodoDraftDeposition,
)
from pudl_archiver.depositors.zenodo.entities import (
    Deposition,
    DepositionCreator,
    DepositionFile,
    DepositionLinks,
    DepositionMetadata,
    FileLinks,
)
from pudl_archiver.utils import RunSettings


def _draft(files: list[DepositionFile]) -> ZenodoDraftDeposition:
    metadata = DepositionMetadata(
        title="PUDL Raw FERC EQR",
        creators=[DepositionCreator(name="Catalyst Cooperative")],
        description="Description",
        license="cc-by-4.0",
        version="1.0.0",
    )
    now = datetime.datetime.now(tz=datetime.UTC)
    deposition = Deposition(
        conceptrecid="123",
        created=now,
        id=123,
        files=files,
        metadata=metadata,
        modified=now,
        links=DepositionLinks(),
        owner=1,
        record_id=123,
        state="unsubmitted",
        submitted=False,
        title=metadata.title,
    )
    return ZenodoDraftDeposition(
        dataset_id="ferceqr",
        settings=RunSettings(depositor="zenodo"),
        api_client=ZenodoAPIClient(sandbox=False),
        deposition=deposition,
    )


def _deposition_file(filename: str, checksum: str) -> DepositionFile:
    return DepositionFile(
        checksum=checksum,
        filename=filename,
        id="fake_id",
        filesize=123,
        links=FileLinks(
            download=f"https://fake.zenodo.org/api/records/123/files/{filename}"
        ),
    )


def _resource(tmp_path: Path, contents: bytes) -> ResourceInfo:
    local_path = tmp_path / "resource.zip"
    local_path.write_bytes(contents)
    return ResourceInfo(local_path=local_path, partitions={})


def test_generate_change_new_file_is_create(tmp_path):
    draft = _draft(files=[])
    resource = _resource(tmp_path, b"new contents")

    change = draft.generate_change("resource.zip", resource)

    assert change.action_type == DepositionAction.CREATE


def test_generate_change_modified_file_is_update(tmp_path):
    draft = _draft(
        files=[_deposition_file("resource.zip", checksum="not-the-real-checksum")]
    )
    resource = _resource(tmp_path, b"new contents")

    change = draft.generate_change("resource.zip", resource)

    assert change.action_type == DepositionAction.UPDATE


def test_generate_change_unchanged_file_is_no_op(tmp_path):
    contents = b"unchanged contents"
    checksum = md5(contents).hexdigest()  # noqa: S324
    draft = _draft(files=[_deposition_file("resource.zip", checksum=checksum)])
    resource = _resource(tmp_path, contents)

    change = draft.generate_change("resource.zip", resource)

    assert change.action_type == DepositionAction.NO_OP

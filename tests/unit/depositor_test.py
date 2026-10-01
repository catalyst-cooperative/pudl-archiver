"""Test the depositor-independent logic in DraftDeposition."""

from hashlib import md5

from pudl_archiver.depositors.depositor import DepositionAction, DraftDeposition
from pudl_archiver.frictionless import ResourceInfo


class _StubDraft(DraftDeposition):
    """Draft whose files are just a mapping of filename to checksum."""

    checksums: dict[str, str]

    def get_checksum(self, filename):
        return self.checksums.get(filename)


# generate_change only needs get_checksum, so skip the other abstract methods.
_StubDraft.__abstractmethods__ = frozenset()


def _generate_change(tmp_path, draft_contents: dict[str, bytes], local: bytes):
    draft = _StubDraft.model_construct(
        checksums={
            name: md5(contents).hexdigest()  # noqa: S324
            for name, contents in draft_contents.items()
        }
    )
    local_path = tmp_path / "resource.zip"
    local_path.write_bytes(local)
    resource = ResourceInfo(local_path=local_path, partitions={})
    return draft.generate_change("resource.zip", resource)


def test_generate_change_new_file_is_create(tmp_path):
    change = _generate_change(tmp_path, {}, b"new")
    assert change.action_type == DepositionAction.CREATE


def test_generate_change_modified_file_is_update(tmp_path):
    change = _generate_change(tmp_path, {"resource.zip": b"old"}, b"new")
    assert change.action_type == DepositionAction.UPDATE


def test_generate_change_unchanged_file_is_no_op(tmp_path):
    change = _generate_change(tmp_path, {"resource.zip": b"same"}, b"same")
    assert change.action_type == DepositionAction.NO_OP

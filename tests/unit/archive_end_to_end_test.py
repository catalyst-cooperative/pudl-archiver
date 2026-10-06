"""Archive a whole fake dataset to a local deposition, and check what is left."""

import pytest

from pudl_archiver import archive_dataset
from pudl_archiver.frictionless import DataPackage


@pytest.mark.asyncio
async def test_archive_of_a_changed_dataset(fake_archive):
    """The second archive of a dataset changes one of its files."""
    summary_file, settings = fake_archive.prepare(
        {"a.txt": b"a contents", "b.txt": b"b contents"}, 1, initialize=True
    )
    await archive_dataset("pudl_test", settings)

    assert fake_archive.load(summary_file).success
    assert set(fake_archive.published()) == {"a.txt", "b.txt", "datapackage.json"}

    summary_file, settings = fake_archive.prepare(
        {"a.txt": b"a, now changed", "b.txt": b"b contents"}, 2
    )
    await archive_dataset("pudl_test", settings)

    summary = fake_archive.load(summary_file)
    assert summary.success
    assert [change.name for change in summary.file_changes] == ["a.txt"]
    # Every file is downloaded in each run, and compared with the last archive
    assert fake_archive.downloaded == ["a.txt", "b.txt", "a.txt", "b.txt"]
    published = fake_archive.published()
    assert published["a.txt"] == b"a, now changed"
    datapackage = DataPackage.model_validate_json(published["datapackage.json"])
    assert {r.name for r in datapackage.resources} == {"a.txt", "b.txt"}
    # The published archive has everything, and the draft is gone
    assert not list(fake_archive.workspace.glob("*"))

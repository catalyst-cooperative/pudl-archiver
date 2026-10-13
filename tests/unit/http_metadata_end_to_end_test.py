"""Archive a fake dataset whose files have HTTP metadata, and test that it is kept."""

import datetime

import pytest

from pudl_archiver import archive_dataset
from pudl_archiver.frictionless import DataPackage, HttpFileMetadata


def _dt(day: int) -> datetime.datetime:
    return datetime.datetime(2026, 10, day, 7, 0, tzinfo=datetime.UTC)


def _file(contents: bytes, day: int | None) -> bytes | tuple[bytes, dict]:
    """A file of the fake dataset, with the HTTP metadata of the day, if there is one."""
    if day is None:
        return contents
    metadata = HttpFileMetadata(
        last_modified=_dt(day), etag=f'"etag-{day}"', content_length=len(contents)
    )
    return contents, {"source_metadata": metadata}


def _recorded_days(fake_archive) -> dict[str, int | None]:
    """The day of the Last-Modified in the published datapackage, of each file."""
    datapackage = DataPackage.model_validate_json(
        fake_archive.published()["datapackage.json"]
    )
    return {
        r.name: r.source_metadata.last_modified.day if r.source_metadata else None
        for r in datapackage.resources
    }


FILES = {
    "a.txt": _file(b"a contents", 1),
    "b.txt": _file(b"b contents", 2),
    "c.txt": _file(b"c contents", 3),
}


@pytest.mark.asyncio
async def test_metadata_is_recorded_and_kept_by_publish_run(fake_archive):
    """`publish-run` regenerates the datapackage, without downloading anything."""
    summary_file, settings = fake_archive.prepare(
        FILES, 1, initialize=True, auto_publish=False
    )
    await archive_dataset("pudl_test", settings)
    summary = fake_archive.load(summary_file)
    assert {n: m.last_modified.day for n, m in summary.source_metadata.items()} == {
        "a.txt": 1,
        "b.txt": 2,
        "c.txt": 3,
    }

    # Like `publish-run`, which inherits the settings of the run that it publishes
    summary_file, settings = fake_archive.prepare(FILES, 2, initialize=True)
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=summary.successful_partitions,
        skip_source_metadata=summary.source_metadata,
    )

    assert fake_archive.load(summary_file).success
    assert _recorded_days(fake_archive) == {"a.txt": 1, "b.txt": 2, "c.txt": 3}


@pytest.mark.asyncio
async def test_metadata_survives_any_number_of_retries(fake_archive):
    """Each retry carries what the earlier ones recorded, even when it fails again."""
    summary_file, settings = fake_archive.prepare(
        FILES, 1, initialize=True, fail="downloading"
    )
    with pytest.raises(RuntimeError, match="Run exception validation test"):
        await archive_dataset("pudl_test", settings)
    summary = fake_archive.load(summary_file)

    # The metadata of what was archived is in the summary of the failed run, and the
    # retries (which fail again) keep it, and don't download what is archived
    for run_number in (2, 3):
        previous = summary
        summary_file, settings = fake_archive.prepare(
            FILES, run_number, initialize=True, fail="downloading"
        )
        with pytest.raises(RuntimeError, match="Run exception validation test"):
            await archive_dataset(
                "pudl_test",
                settings,
                skip_partitions=previous.successful_partitions,
                skip_source_metadata=previous.source_metadata,
            )
        summary = fake_archive.load(summary_file)
        assert set(summary.source_metadata) == {"a.txt", "b.txt"}

    # The retry that finishes has the metadata of all three
    fake_archive.downloaded.clear()
    summary_file, settings = fake_archive.prepare(FILES, 4, initialize=True)
    await archive_dataset(
        "pudl_test",
        settings,
        skip_partitions=summary.successful_partitions,
        skip_source_metadata=summary.source_metadata,
    )

    assert fake_archive.load(summary_file).success
    assert fake_archive.downloaded == ["c.txt"]
    assert _recorded_days(fake_archive) == {"a.txt": 1, "b.txt": 2, "c.txt": 3}


@pytest.mark.asyncio
async def test_metadata_of_a_changed_file_isnt_kept_if_it_wasnt_observed(fake_archive):
    """What was recorded for the old contents doesn't describe the new contents."""
    _, settings = fake_archive.prepare(FILES, 1, initialize=True)
    await archive_dataset("pudl_test", settings)

    # a.txt changes, but we couldn't get its metadata. b.txt and c.txt didn't change.
    files = FILES | {"a.txt": b"a, now changed"}
    _, settings = fake_archive.prepare(files, 2)
    await archive_dataset("pudl_test", settings)

    assert _recorded_days(fake_archive) == {"a.txt": None, "b.txt": 2, "c.txt": 3}

"""Real, network-hitting tests against the live EPA bulk-files API.

Unlike tests/unit/epa_archiver_test.py (which mocks the aiohttp session),
these hit the actual EPA API to catch drift in its response shape/format --
exactly the kind of thing that made us drop the old single-quote JSON cleanup
after confirming the live API no longer needs it.
"""

import aiohttp
import pytest
from pydantic import ValidationError

from pudl_archiver.archivers.epa.epacems import BulkFile, EpaCemsArchiver
from pudl_archiver.archivers.epa.epamats import EpaMatsArchiver


def _parse_bulk_files(items: list[dict]) -> list[BulkFile]:
    """Parse bulk-file entries, silently dropping incomplete ones.

    Mirrors the archivers' own __filter_for_complete_metadata, e.g. the
    per-state-year aggregate files that have no "quarter" field.
    """
    bulk_files = []
    for item in items:
        try:
            bulk_files.append(BulkFile(**item))
        except ValidationError:
            continue
    return bulk_files


@pytest.mark.parametrize(
    "archiver_cls,valid_data_type",
    [
        (EpaCemsArchiver, "Emissions"),
        (EpaMatsArchiver, "Mercury and Air Toxics Emissions (MATS)"),
    ],
)
@pytest.mark.asyncio
async def test_bulk_files_api_shape(archiver_cls, valid_data_type):
    """Confirm the real bulk-files API still returns the shape we expect.

    Cheap (a couple seconds, no large downloads): just fetches and parses
    the file listing, the same way get_resources does. This is the layer
    that would catch the API reverting to non-standard JSON, renaming
    fields, or otherwise changing shape underneath us.
    """
    async with aiohttp.ClientSession() as session:
        archiver = archiver_cls(session)
        response = await archiver.get_json(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=archiver.parameters,
        )
        assert "items" in response
        assert len(response["items"]) > 1000  # sanity check, not brittle

        bulk_files = _parse_bulk_files(response["items"])

        quarterly = [
            f
            for f in bulk_files
            if f.metadata.data_type == valid_data_type
            and f.metadata.data_sub_type == "Hourly"
            and f.metadata.quarter in {1, 2, 3, 4}
        ]
        assert len(quarterly) > 0


@pytest.mark.asyncio
async def test_download_single_quarter():
    """Download one real, small quarter end to end and sanity-check it.

    Exercises the full async pipeline (get_json -> pydantic validation ->
    download_file -> zip archiving) against the live API. Pinned to 1996 Q2,
    the smallest available national quarterly file (~213 MB as of writing),
    to keep this as cheap as a real download of this data can be.
    """
    async with aiohttp.ClientSession() as session:
        archiver = EpaCemsArchiver(session, only_years=[1996])
        response = await archiver.get_json(
            "https://api.epa.gov/easey/camd-services/bulk-files",
            params=archiver.parameters,
        )
        bulk_files = _parse_bulk_files(response["items"])

        [one_quarter] = [
            f
            for f in bulk_files
            if f.metadata.data_type == "Emissions"
            and f.metadata.data_sub_type == "Hourly"
            and f.metadata.year == 1996
            and f.metadata.quarter == 2
        ]

        resource_info = await archiver.get_year_resource(1996, [one_quarter])

        assert resource_info.local_path.exists()
        assert resource_info.partitions == {"year_quarter": ["1996q2"]}
        # Not empty/truncated -- the real file is ~213 MB, well-compressed CSV.
        assert resource_info.local_path.stat().st_size > 1_000_000

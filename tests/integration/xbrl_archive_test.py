"""Test generic FERC XBRL archive methods."""

import random
import zipfile
from pathlib import Path

import aiohttp
import pytest

from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.archivers.ferc import xbrl
from pudl_archiver.archivers.ferc.ferc1 import Ferc1Archiver
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.orchestrator import orchestrate_run
from pudl_archiver.utils import RunSettings


async def _download_filing_mock(
    filing: xbrl.FeedEntry,
    session: aiohttp.ClientSession,
) -> bytes:
    unique_hash = str(hash(filing.download_url))
    filing_data = (
        unique_hash
        + "\nhttps://ecollection.ferc.gov/taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"
    )

    return filing_data.encode("utf-8")


async def archive_taxnomies_mock(
    taxonomies_referenced: set[str],
    form: xbrl.FercForm,
    output_dir: Path,
    session: aiohttp.ClientSession,
):
    archive_path = output_dir / f"ferc{form.as_int()}-xbrl-taxonomies.zip"
    taxonomies = []

    with zipfile.ZipFile(
        archive_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for taxonomy in taxonomies_referenced:
            taxonomy_zip_name = xbrl._taxonomy_zip_name_from_url(taxonomy)
            taxonomies.append(taxonomy_zip_name)
            with archive.open(taxonomy_zip_name, mode="w") as taxonomy_zip:
                taxonomy_zip.write(taxonomy.encode("utf-8"))
    return ResourceInfo(
        local_path=archive_path,
        partitions={
            "taxonomy_versions": taxonomies,
            "data_format": "XBRL_TAXONOMY",
        },
        layout=ZipLayout(file_paths=taxonomies),
    )


@pytest.mark.asyncio
async def test_archive_year_stability(mocker, tmp_path):
    """Test that running ``archive_year`` multiple times with identical filings produces stable archives."""
    mocker.patch(
        "pudl_archiver.archivers.ferc.xbrl._download_filing",
        new=_download_filing_mock,
    )
    mocker.patch(
        "pudl_archiver.archivers.validate._validate_file_type",
        new=lambda _path, _required_for_run_success: True,
    )
    mocker.patch(
        "pudl_archiver.archivers.ferc.xbrl.archive_taxonomies",
        new=archive_taxnomies_mock,
    )

    # Get all rss feeds for indexing
    rss_feeds = xbrl._get_rss_feeds()
    # Return rss feeds in random order, as previously this would have caused the
    # Archives to look like they had changed when there were no substantive changes
    mocker.patch(
        "pudl_archiver.archivers.ferc.xbrl._get_rss_feeds",
        new=lambda: random.sample(rss_feeds, len(rss_feeds)),
    )

    async with aiohttp.ClientSession() as session:
        # Run once
        summary, _ = await orchestrate_run(
            dataset="ferc1",
            downloader=Ferc1Archiver(
                session,
                only_years=[2022, 2023, 2024],
            ),
            run_settings=RunSettings(
                sandbox=False,
                initialize=True,
                deposition_path=str(tmp_path),
                auto_publish=True,
                depositor="fsspec",
            ),
            session=session,
        )

        # Run again and validate stability
        summary, _ = await orchestrate_run(
            dataset="ferc1",
            downloader=Ferc1Archiver(
                session,
                only_years=[2022, 2023, 2024],
            ),
            run_settings=RunSettings(
                sandbox=False,
                initialize=False,
                deposition_path=str(tmp_path),
                auto_publish=True,
                depositor="fsspec",
            ),
            session=session,
        )

        assert len(summary.file_changes) == 0

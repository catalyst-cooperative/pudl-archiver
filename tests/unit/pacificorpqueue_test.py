import ssl
import zipfile

import pytest

from pudl_archiver.archivers.pacificorpqueue import (
    CANDIDATE_QUEUE_PAGES,
    KNOWN_QUEUE_PAGES,
    PacifiCorpQueueArchiver,
    oati_ssl_context,
)


def test_oati_ssl_context_keeps_verification_on():
    context = oati_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
    # Only OATI's intermediate is trusted, not the public trust store.
    assert len(context.get_ca_certs()) == 1


@pytest.mark.asyncio
async def test_get_resources_archives_known_and_new_pages(mocker):
    new_url = CANDIDATE_QUEUE_PAGES["cluster_6"]
    page = b"<html><body>PacifiCorp Generation Interconnection Requests</body></html>"

    async def fake_fetch_page(url, ssl_context):
        if url in KNOWN_QUEUE_PAGES.values() or url == new_url:
            return 200, page
        return 404, b"Not Found"

    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    mocker.patch.object(archiver, "fetch_page", side_effect=fake_fetch_page)

    resources = [await res async for res in archiver.get_resources()]

    partitions = {r.partitions["queue"] for r in resources}
    assert partitions == set(KNOWN_QUEUE_PAGES) | {"cluster_6"}

    # Each page is zipped under its original file name, with contents unchanged.
    for resource in resources:
        with zipfile.ZipFile(resource.local_path) as archive:
            (name,) = archive.namelist()
            assert name.endswith(".htm")
            assert archive.read(name) == page


@pytest.mark.asyncio
async def test_missing_known_page_raises(mocker):
    async def fake_fetch_page(url, ssl_context):
        if url == KNOWN_QUEUE_PAGES["cluster_2"]:
            return 404, b"Not Found"
        return 200, b"<html></html>"

    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    mocker.patch.object(archiver, "fetch_page", side_effect=fake_fetch_page)

    with pytest.raises(RuntimeError, match="returned HTTP 404"):
        await archiver.get_queue_page(
            "cluster_2", KNOWN_QUEUE_PAGES["cluster_2"], ssl_context=None
        )

import zipfile

import pytest

from pudl_archiver.archivers.pacificorpqueue import (
    PacifiCorpQueueArchiver,
    other_partition_name,
    partition_name,
    queue_pages,
)

BASE = "https://www.oasis.oati.com/woa/docs/PPW/PPWdocs"


def document(title: str, file_name: str, url: str | None = None) -> dict:
    """Build a menu entry for one document, shaped like the OASIS menu's JSON."""
    url = url or f"{BASE}/{file_name}"
    return {
        "textValue": title,
        "FileName": file_name,
        "leaf": True,
        "children": None,
        "text": (
            f"<span title='{title}'>{title}</span>"
            f'<div class="popup" onclick="CopyToClipboard(this,\'{url}\')"></div>'
        ),
    }


def folder(title: str, children: list[dict]) -> dict:
    """Build a menu folder, shaped like the OASIS menu's JSON."""
    return {"textValue": title, "FileName": None, "leaf": False, "children": children}


def menu(
    cluster_queue_entries: list[dict], other_folders: list[dict] | None = None
) -> dict:
    """Build a whole menu with the Cluster Queues folder two levels down."""
    return {
        "success": True,
        "children": [
            folder("ATC Information", [document("ATC", "atc.htm")]),
            folder(
                "Generation Interconnection",
                [
                    folder("Applications", []),
                    folder("Cluster Queues", cluster_queue_entries),
                    *(other_folders or []),
                ],
            ),
        ],
    }


def test_partition_name():
    assert partition_name("Cluster Study 4 - Queue") == "cluster-study-4"
    assert partition_name("Transition Cluster - Queue") == "transition-cluster"
    assert partition_name("Transition Cluster 2 - Queue") == "transition-cluster-2"


def test_queue_pages_keeps_only_queue_documents():
    tree = menu(
        [
            document("Transition Cluster - Queue", "pacificorpcliaq.htm"),
            document("Cluster Study 4 - Queue", "pacificorpcliaq4.htm"),
            document(
                "Cluster Study 4 - Cluster Studies", "Cluster4_ClusterStudies.htm"
            ),
            folder("Cluster 4 Information", [document("Report - Queue", "r.htm")]),
        ]
    )
    assert queue_pages(tree) == {
        "transition-cluster": f"{BASE}/pacificorpcliaq.htm",
        "cluster-study-4": f"{BASE}/pacificorpcliaq4.htm",
    }


def test_other_partition_name():
    assert (
        other_partition_name("Serial Queue", "Generation Interconnection Queue")
        == "serial"
    )
    assert other_partition_name("Fast Track Queue", "Fast Track Queue") == "fast-track"
    assert (
        other_partition_name("Serial Queue", "Archive - Withdrawn")
        == "serial-archive-withdrawn"
    )


def test_queue_pages_includes_the_other_queue_folders():
    tree = menu(
        [document("Cluster Study 4 - Queue", "pacificorpcliaq4.htm")],
        [
            folder(
                "Serial Queue",
                [
                    document("Generation Interconnection Queue", "pacificorplgiaq.htm"),
                    document("Archive - Withdrawn", "pacificorplgiaqw.htm"),
                    document("Application", "application.docx"),
                ],
            ),
            folder(
                "Surplus Queue", [document("Surplus Queue", "pacificorpslgiaq.htm")]
            ),
            # Folders whose names do not end in "Queue" are not queues.
            folder("Queue Statistics", [document("2026", "pacificorpstats.htm")]),
            folder("Model Files", [document("Model File Access", "models.pdf")]),
        ],
    )
    assert queue_pages(tree) == {
        "cluster-study-4": f"{BASE}/pacificorpcliaq4.htm",
        "serial": f"{BASE}/pacificorplgiaq.htm",
        "serial-archive-withdrawn": f"{BASE}/pacificorplgiaqw.htm",
        "surplus": f"{BASE}/pacificorpslgiaq.htm",
    }


def test_queue_pages_rejects_two_pages_with_the_same_partition():
    tree = menu(
        [],
        [
            folder(
                "Surplus Queue",
                [
                    document("Surplus Queue", "pacificorpslgiaq.htm"),
                    document("Older Surplus Queue", "pacificorpslgiaq2.htm"),
                ],
            )
        ],
    )
    with pytest.raises(RuntimeError, match="share the partition 'surplus'"):
        queue_pages(tree)


def test_queue_pages_rejects_addresses_off_the_oasis_site():
    tree = menu(
        [
            document(
                "Cluster Study 5 - Queue",
                "pacificorpcliaq5.htm",
                url="https://example.com/pacificorpcliaq5.htm",
            )
        ]
    )
    with pytest.raises(RuntimeError, match="points off the OASIS site"):
        queue_pages(tree)


def test_queue_pages_rejects_address_that_does_not_match_file_name():
    tree = menu(
        [
            document(
                "Cluster Study 5 - Queue",
                "pacificorpcliaq5.htm",
                url=f"{BASE}/something_else.htm",
            )
        ]
    )
    with pytest.raises(RuntimeError, match="does not match file"):
        queue_pages(tree)


def test_queue_pages_fails_if_folder_is_missing():
    tree = {"success": True, "children": [folder("ATC Information", [])]}
    with pytest.raises(RuntimeError, match="No 'Generation Interconnection' folder"):
        queue_pages(tree)


def test_queue_pages_fails_if_folder_has_no_queues():
    tree = menu([document("Cluster Study 4 - Cluster Studies", "c4.htm")])
    with pytest.raises(RuntimeError, match="No queue pages found"):
        queue_pages(tree)


@pytest.mark.asyncio
async def test_get_resources_archives_every_page_in_the_menu(mocker):
    tree = menu(
        [
            document("Transition Cluster - Queue", "pacificorpcliaq.htm"),
            document("Cluster Study 4 - Queue", "pacificorpcliaq4.htm"),
        ]
    )
    page = b"<html><body>PacifiCorp Generation Interconnection Requests</body></html>"

    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    mocker.patch.object(archiver, "start_browser")
    mocker.patch.object(archiver, "get_menu_tree", return_value=tree)
    mocker.patch.object(archiver, "fetch_page", return_value=(200, page))

    resources = [await res async for res in archiver.get_resources()]

    assert {r.partitions["queue"] for r in resources} == {
        "transition-cluster",
        "cluster-study-4",
    }
    # Each page is zipped under its original file name, with contents unchanged.
    for resource in resources:
        with zipfile.ZipFile(resource.local_path) as archive:
            (name,) = archive.namelist()
            assert name.startswith("pacificorpcliaq")
            assert archive.read(name) == page


@pytest.mark.asyncio
async def test_missing_queue_page_raises(mocker):
    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    mocker.patch.object(archiver, "fetch_page", return_value=(404, b"Not Found"))

    with pytest.raises(RuntimeError, match="returned HTTP 404"):
        await archiver.get_queue_page("cluster-study-4", f"{BASE}/pacificorpcliaq4.htm")


def browser_returning(mocker, archiver, response):
    """Give the archiver a browser whose pages load to this response."""
    page = mocker.AsyncMock()
    page.goto.return_value = response
    page.content.return_value = "<html>the browser's rendering</html>"
    archiver.browser_context = mocker.AsyncMock()
    archiver.browser_context.new_page.return_value = page
    return page


@pytest.mark.asyncio
async def test_fetch_page_keeps_the_bytes_the_server_sent(mocker):
    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    url = f"{BASE}/pacificorpcliaq4.htm"
    response = mocker.Mock(url=url, status=200)
    response.body = mocker.AsyncMock(return_value=b"<html>as published</html>")
    page = browser_returning(mocker, archiver, response)

    assert await archiver.fetch_page(url) == (200, b"<html>as published</html>")
    page.content.assert_not_awaited()
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_page_rejects_a_redirect_off_the_oasis_site(mocker):
    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    response = mocker.Mock(url="https://example.com/pacificorpcliaq4.htm", status=200)
    page = browser_returning(mocker, archiver, response)

    with pytest.raises(RuntimeError, match="redirected off the OASIS site"):
        await archiver.fetch_page(f"{BASE}/pacificorpcliaq4.htm")
    page.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_after_download_is_safe_if_the_browser_never_started(mocker):
    archiver = PacifiCorpQueueArchiver(mocker.AsyncMock())
    await archiver.after_download()

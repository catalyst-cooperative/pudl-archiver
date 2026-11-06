"""Archive PacifiCorp's generation interconnection queue postings.

PacifiCorp publishes its generator interconnection queues on its Open Access
Same-Time Information System (OASIS) site, which is hosted by Open Access Technology
International (OATI). Each queue is a static HTML page in the "Generation
Interconnection" folder of the OASIS document menu: one page per cluster study under
"Cluster Queues", and one or more pages in each of the other queue folders (Serial,
Surplus, Provisional, Fast Track, and so on). The Serial Queue folder also holds
archive pages listing requests that were withdrawn, went into service, or have an
executed agreement but are not yet in service. Each page is archived exactly as
published.

The site is read with a headless browser, for two reasons. The document menu is built
in the browser from a JSON request (``HomePage/GetData``) that the OASIS application
answers only for real browsers; other HTTP clients receive HTTP 403. And OATI's
certificate chain for www.oasis.oati.com is not trusted by Python's or Chromium's
default trust stores, so the browser skips certificate checks. To limit what that
allows, only HTTPS addresses on the OATI host are accepted from the menu, and a page
that loads from any other host is rejected.

Each page is saved as the bytes the server sent, not as the browser's rendering of it.

The archiver works in three steps, and the module is laid out in the same order:

1. Find the queue pages. :meth:`PacifiCorpQueueArchiver.get_menu_tree` loads the OASIS
   menu, and :func:`queue_pages` walks it to list the queue pages, keyed by partition.
2. Validate and name them. :func:`page_url` takes each page's address from the menu and
   checks it with :func:`validate_url`; :func:`partition_name` and
   :func:`other_partition_name` turn menu titles into partition names.
3. Download them. :meth:`PacifiCorpQueueArchiver.fetch_page` loads each page, checks
   with :func:`validate_url` that it did not redirect off the OASIS site, and
   :meth:`PacifiCorpQueueArchiver.get_queue_page` zips it unchanged.
"""

import re
from io import BytesIO
from urllib.parse import urlparse

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright
from playwright.async_api import Error as PlaywrightError

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.utils import retry_async

OASIS_HOST = "www.oasis.oati.com"
MENU_PAGE_URL = (
    f"https://{OASIS_HOST}/webSmartOASIS/HomePage?ProviderName=PPW&Homepage=1"
)
MENU_DATA_PATH = "/HomePage/GetData"
PAGE_TIMEOUT_MS = 10 * 60 * 1000
"""Ten minutes, as for the other Playwright downloads and the aiohttp session.

Some pages take far longer than Playwright's 30-second default to load: the serial
queue's withdrawn archive (pacificorplgiaqw.htm) lists every withdrawn request.
"""
GI_FOLDER = "Generation Interconnection"
CLUSTER_FOLDER = "Cluster Queues"
QUEUE_SUFFIX = " - Queue"
QUEUE_PAGE_PATTERN = re.compile(r"^pacificorp\w*\.htm$", re.IGNORECASE)
"""Queue pages outside the cluster folder are all named like pacificorplgiaq.htm."""

COPY_URL_PATTERN = re.compile(r"CopyToClipboard\(this,'([^']+)'\)")
"""Each menu entry's full address appears in its copy-to-clipboard button."""


# Step 1: find the queue pages in the OASIS menu.


def find_folder(nodes: list[dict] | None, name: str) -> dict | None:
    """Search the menu tree, depth first, for the folder with this name."""
    for node in nodes or []:
        if node.get("textValue") == name and not node.get("leaf"):
            return node
        found = find_folder(node.get("children"), name)
        if found is not None:
            return found
    return None


def folder_pages(folder: dict) -> list[tuple[str, dict]]:
    """List the queue pages in one folder of Generation Interconnection."""
    name = (folder.get("textValue") or "").strip()
    is_cluster = name == CLUSTER_FOLDER
    if not is_cluster and not name.endswith("Queue"):
        return []
    pages = []
    for node in folder.get("children") or []:
        if not node.get("leaf"):
            continue
        title = (node.get("textValue") or "").strip()
        if is_cluster and title.endswith(QUEUE_SUFFIX):
            pages.append((partition_name(title), node))
        elif not is_cluster and QUEUE_PAGE_PATTERN.match(node.get("FileName") or ""):
            pages.append((other_partition_name(name, title), node))
    return pages


def queue_pages(tree: dict) -> dict[str, str]:
    """List the queue pages in the OASIS menu, keyed by partition name.

    Takes every "<cluster> - Queue" page in the Cluster Queues folder, and every
    pacificorp*.htm page in the other folders of Generation Interconnection whose names
    end in "Queue". Applications, studies and other documents are skipped. Each page's
    address is validated with :func:`page_url` before it is returned.
    """
    gi = find_folder(tree.get("children"), GI_FOLDER)
    if gi is None:
        raise RuntimeError(
            f"No '{GI_FOLDER}' folder in the OASIS menu; its layout may have changed."
        )

    found = [
        entry
        for folder in gi.get("children") or []
        if not folder.get("leaf")
        for entry in folder_pages(folder)
    ]

    pages: dict[str, str] = {}
    for partition, node in found:
        if partition in pages:
            raise RuntimeError(
                f"Two queue pages would share the partition {partition!r}."
            )
        pages[partition] = page_url(node)

    if not pages:
        raise RuntimeError(f"No queue pages found in the '{GI_FOLDER}' folder.")
    return pages


# Step 2: validate each page's address and name its partition.


def validate_url(url: str, source: str) -> str:
    """Check that an address is an HTTPS page on the OASIS site, and return it.

    This is the one check on where the archiver may go. It is applied twice: to each
    address taken from the menu (:func:`page_url`), and to the address each page
    finally loads from, after any redirects
    (:meth:`PacifiCorpQueueArchiver.fetch_page`). It matters because the browser skips
    certificate checks for OATI's host.

    Args:
        url: the address to check.
        source: where the address came from, for the error message.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != OASIS_HOST:
        raise RuntimeError(f"{source} is off the OASIS site: {url}")
    return url


def page_url(node: dict) -> str:
    """Return a menu entry's address, after validating it.

    The address must pass :func:`validate_url` and must end in the file name the menu
    gives for the entry.
    """
    title = node.get("textValue") or ""
    match = COPY_URL_PATTERN.search(node.get("text") or "")
    if match is None:
        raise RuntimeError(f"No address found for menu entry {title!r}.")
    url = validate_url(match.group(1), f"Menu entry {title!r}")

    file_name = node.get("FileName")
    if file_name and not urlparse(url).path.endswith(f"/{file_name}"):
        raise RuntimeError(
            f"Menu entry {title!r}: address {url} does not match file {file_name}."
        )
    return url


def slug(text: str) -> str:
    """Lower-case a menu title and join its words with hyphens."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def partition_name(title: str) -> str:
    """Turn a cluster queue's menu title into a partition name.

    For example, "Cluster Study 4 - Queue" becomes "cluster-study-4".
    """
    return slug(title.removesuffix(QUEUE_SUFFIX))


def other_partition_name(folder: str, title: str) -> str:
    """Name a page in one of the other queue folders.

    A folder's main page takes the folder's name: "Serial Queue" > "Generation
    Interconnection Queue" becomes "serial". Other pages add their own title:
    "Serial Queue" > "Archive - Withdrawn" becomes "serial-archive-withdrawn".
    """
    base = slug(folder.removesuffix(" Queue"))
    if title.endswith("Queue"):
        return base
    return f"{base}-{slug(title)}"


# Step 3: download each page (the archiver class below).


class PacifiCorpQueueArchiver(AbstractDatasetArchiver):
    """PacifiCorp generation interconnection queue archiver."""

    name = "pacificorpqueue"
    concurrency_limit = 1  # A handful of small pages; no need to hurry the server.
    allowed_file_rel_diff = (
        0.5  # A single update can move many requests between clusters, so allow
    )
    # larger changes in page size than the default before flagging them.

    # Set by start_browser; None until then, so cleanup is safe if starting fails.
    playwright: Playwright | None = None
    browser: Browser | None = None
    browser_context: BrowserContext | None = None

    async def get_resources(self) -> ArchiveAwaitable:
        """Download every queue page listed in the OASIS menu."""
        await self.start_browser()

        # Steps 1 and 2: find the queue pages in the menu, validate their addresses
        # and name their partitions.
        pages = queue_pages(await self.get_menu_tree())
        self.logger.info(f"Found PacifiCorp queue pages: {sorted(pages)}")

        # Step 3: download each page.
        for partition, url in pages.items():
            yield self.get_queue_page(partition, url)

    async def start_browser(self) -> None:
        """Start a headless browser that skips OATI's unverifiable certificate.

        Every page load and wait in the browser uses :data:`PAGE_TIMEOUT_MS`.
        """
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch()
        self.browser_context = await self.browser.new_context(ignore_https_errors=True)
        self.browser_context.set_default_timeout(PAGE_TIMEOUT_MS)

    async def after_download(self) -> None:
        """Close the browser once every page has been downloaded."""
        if self.browser is not None:
            await self.browser.close()
        if self.playwright is not None:
            await self.playwright.stop()

    async def get_menu_tree(self) -> dict:
        """Load the OASIS home page and return the menu it requests."""
        page = await self.browser_context.new_page()
        try:
            async with page.expect_response(
                lambda response: MENU_DATA_PATH in response.url
            ) as response_info:
                await page.goto(MENU_PAGE_URL)
            response = await response_info.value
            if response.status != 200:
                raise RuntimeError(
                    f"OASIS menu request returned HTTP {response.status}."
                )
            return await response.json()
        finally:
            await page.close()

    async def fetch_page(self, url: str) -> tuple[int, bytes]:
        """Load a page from the OATI OASIS site, returning its status and contents.

        The page must finish loading on the OASIS site; a redirect elsewhere is
        rejected by :func:`validate_url`. The contents are the response body exactly as
        the server sent it. The page's ``content()`` is not used, because that is the
        browser's re-serialisation of the page after parsing, not the file as
        published.
        """
        page = await self.browser_context.new_page()
        try:
            response = await retry_async(
                page.goto, args=[url], retry_on=(PlaywrightError,)
            )
            if response is None:
                raise RuntimeError(f"No response when loading {url}.")
            validate_url(response.url, f"Queue page {url} redirected, and its target")
            return response.status, await response.body()
        finally:
            await page.close()

    async def get_queue_page(self, partition: str, url: str) -> ResourceInfo:
        """Download one queue page and zip it, keeping the original file unchanged."""
        status, body = await self.fetch_page(url)
        if status != 200:
            raise RuntimeError(f"Queue page {url} returned HTTP {status}.")

        filename = url.rsplit("/", 1)[-1]
        zip_path = self.download_directory / f"{self.name}-{partition}.zip"
        self.add_to_archive(zip_path=zip_path, filename=filename, blob=BytesIO(body))
        return ResourceInfo(local_path=zip_path, partitions={"queue": partition})

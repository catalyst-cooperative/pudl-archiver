"""Archive PacifiCorp's generation interconnection queue postings.

PacifiCorp publishes its generator interconnection queue on its Open Access
Same-Time Information System (OASIS) site, which is hosted by Open Access Technology
International (OATI). Each cluster's queue is a static HTML page, listed in the
"Generation Interconnection > Cluster Queues" folder of the OASIS document menu.
Each page is archived exactly as published.

The document menu is built in the browser from a JSON request (``HomePage/GetData``)
that the OASIS application answers only for real browsers; other HTTP clients receive
HTTP 403. The menu is therefore read with a headless browser, and the queue pages it
lists are then downloaded directly.

OATI's certificate chain for www.oasis.oati.com is not trusted by Python's or
Chromium's default trust stores. Queue pages are downloaded trusting OATI's
intermediate certificate, stored in ``pudl_archiver/package_data``, with signature,
expiry and hostname checks still in place. That file will need replacing when OATI
changes its intermediate; the current one expires on 2030-10-09. The browser that
reads the menu cannot be given that certificate, so it skips certificate checks. To
limit what that allows, only HTTPS addresses on the OATI host are accepted from the
menu, and every page is then downloaded with full verification.
"""

import importlib.resources
import re
import ssl
from io import BytesIO
from urllib.parse import urlparse

from playwright.async_api import async_playwright

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
QUEUE_FOLDER = "Cluster Queues"
QUEUE_SUFFIX = " - Queue"
OATI_CA_FILE = "oati_webcares_server_issuing_ca_2025.pem"

COPY_URL_PATTERN = re.compile(r"CopyToClipboard\(this,'([^']+)'\)")
"""Each menu entry's full address appears in its copy-to-clipboard button."""


def oati_ssl_context() -> ssl.SSLContext:
    """Build an SSL context that trusts OATI's intermediate certificate only."""
    ca_pem = (
        importlib.resources.files("pudl_archiver.package_data") / OATI_CA_FILE
    ).read_text()
    context = ssl.create_default_context(cadata=ca_pem)
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    return context


def find_folder(nodes: list[dict] | None, name: str) -> dict | None:
    """Search the menu tree, depth first, for the folder with this name."""
    for node in nodes or []:
        if node.get("textValue") == name and not node.get("leaf"):
            return node
        found = find_folder(node.get("children"), name)
        if found is not None:
            return found
    return None


def partition_name(title: str) -> str:
    """Turn a menu title into a partition name.

    For example, "Cluster Study 4 - Queue" becomes "cluster_study_4".
    """
    base = title.removesuffix(QUEUE_SUFFIX)
    return re.sub(r"[^a-z0-9]+", "_", base.lower()).strip("_")


def queue_pages(tree: dict) -> dict[str, str]:
    """List the queue pages in the OASIS menu, keyed by partition name."""
    folder = find_folder(tree.get("children"), QUEUE_FOLDER)
    if folder is None:
        raise RuntimeError(
            f"No '{QUEUE_FOLDER}' folder in the OASIS menu; its layout may have changed."
        )

    pages = {}
    for node in folder.get("children") or []:
        title = node.get("textValue") or ""
        if not node.get("leaf") or not title.endswith(QUEUE_SUFFIX):
            continue

        match = COPY_URL_PATTERN.search(node.get("text") or "")
        if match is None:
            raise RuntimeError(f"No address found for menu entry {title!r}.")
        url = match.group(1)

        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname != OASIS_HOST:
            raise RuntimeError(f"Menu entry {title!r} points off the OASIS site: {url}")
        file_name = node.get("FileName")
        if file_name and not parsed.path.endswith(f"/{file_name}"):
            raise RuntimeError(
                f"Menu entry {title!r}: address {url} does not match file {file_name}."
            )

        pages[partition_name(title)] = url

    if not pages:
        raise RuntimeError(f"No queue pages found in the '{QUEUE_FOLDER}' folder.")
    return pages


class PacifiCorpQueueArchiver(AbstractDatasetArchiver):
    """PacifiCorp generation interconnection queue archiver."""

    name = "pacificorpqueue"
    concurrency_limit = 1  # A handful of small pages; no need to hurry the server.
    allowed_file_rel_diff = (
        0.5  # A single update can move many requests between clusters, so allow
    )
    # larger changes in page size than the default before flagging them.

    async def get_resources(self) -> ArchiveAwaitable:
        """Download every queue page listed in the OASIS menu."""
        pages = queue_pages(await self.get_menu_tree())
        self.logger.info(f"Found PacifiCorp queue pages: {sorted(pages)}")

        ssl_context = oati_ssl_context()
        for partition, url in pages.items():
            yield self.get_queue_page(partition, url, ssl_context)

    async def get_menu_tree(self) -> dict:
        """Load the OASIS home page in a browser and return the menu it requests."""
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            try:
                page = await browser.new_page(ignore_https_errors=True)
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
                await browser.close()

    async def fetch_page(
        self, url: str, ssl_context: ssl.SSLContext
    ) -> tuple[int, bytes]:
        """Fetch a page from the OATI OASIS site, returning its status and contents."""
        response = await retry_async(
            self.session.get, args=[url], kwargs={"ssl": ssl_context}
        )
        body = await retry_async(response.read)
        return response.status, body

    async def get_queue_page(
        self, partition: str, url: str, ssl_context: ssl.SSLContext
    ) -> ResourceInfo:
        """Download one queue page and zip it, keeping the original file unchanged."""
        status, body = await self.fetch_page(url, ssl_context)
        if status != 200:
            raise RuntimeError(f"Queue page {url} returned HTTP {status}.")

        filename = url.rsplit("/", 1)[-1]
        zip_path = self.download_directory / f"{self.name}-{partition}.zip"
        self.add_to_archive(zip_path=zip_path, filename=filename, blob=BytesIO(body))
        return ResourceInfo(local_path=zip_path, partitions={"queue": partition})

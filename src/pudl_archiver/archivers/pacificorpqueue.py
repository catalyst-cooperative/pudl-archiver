"""Archive PacifiCorp's generation interconnection queue postings.

PacifiCorp publishes its generator interconnection queue on its Open Access
Same-Time Information System (OASIS) site, which is hosted by Open Access Technology
International (OATI). The queue is split across one static HTML page per cluster,
plus an older page for the retired serial queue. Each page is archived exactly as
published.

OATI serves www.oasis.oati.com with a certificate issued by its own certificate
authority (part of the NAESB energy-industry PKI), which is not included in the
public trust stores used by Python. Requests to this site therefore trust OATI's
intermediate certificate, stored in ``pudl_archiver/package_data``, with signature,
expiry and hostname checks still in place. That file will need replacing when OATI
changes its intermediate; the current one expires on 2030-10-09.
"""

import importlib.resources
import ssl
from io import BytesIO

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.utils import retry_async

OASIS_DOCS_URL = "https://www.oasis.oati.com/woa/docs/PPW/PPWdocs"
OATI_CA_FILE = "oati_webcares_server_issuing_ca_2025.pem"

KNOWN_QUEUE_PAGES: dict[str, str] = {
    "transition_cluster": f"{OASIS_DOCS_URL}/pacificorpcliaq.htm",
    "cluster_1": f"{OASIS_DOCS_URL}/pacificorpcliaq1.htm",
    "cluster_2": f"{OASIS_DOCS_URL}/pacificorpcliaq2.htm",
    "cluster_3": f"{OASIS_DOCS_URL}/pacificorpcliaq3.htm",
    "cluster_4": f"{OASIS_DOCS_URL}/pacificorpcliaq4.htm",
    "transition_cluster_2": f"{OASIS_DOCS_URL}/pacificorpcliaqT2.htm",
    "serial": "https://www.oasis.oati.com/PPW/PPWdocs/pacificorplgiaq.htm",
}
"""Queue pages known to exist, keyed by the partition name used in the archive."""

CANDIDATE_QUEUE_PAGES: dict[str, str] = {
    f"cluster_{n}": f"{OASIS_DOCS_URL}/pacificorpcliaq{n}.htm" for n in range(5, 10)
} | {
    f"transition_cluster_{n}": f"{OASIS_DOCS_URL}/pacificorpcliaqT{n}.htm"
    for n in range(3, 5)
}
"""Pages that PacifiCorp may add as new clusters open, following its naming pattern.

These return HTTP 404 until they exist. Any that are found are archived and logged,
so that they can be moved into ``KNOWN_QUEUE_PAGES``.
"""


def oati_ssl_context() -> ssl.SSLContext:
    """Build an SSL context that trusts OATI's intermediate certificate only."""
    ca_pem = (
        importlib.resources.files("pudl_archiver.package_data") / OATI_CA_FILE
    ).read_text()
    context = ssl.create_default_context(cadata=ca_pem)
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    return context


class PacifiCorpQueueArchiver(AbstractDatasetArchiver):
    """PacifiCorp generation interconnection queue archiver."""

    name = "pacificorpqueue"
    concurrency_limit = 1  # A handful of small pages; no need to hurry the server.

    async def get_resources(self) -> ArchiveAwaitable:
        """Download every known queue page, plus any new cluster pages found."""
        ssl_context = oati_ssl_context()
        pages = dict(KNOWN_QUEUE_PAGES)

        new_pages = await self.find_new_queue_pages(ssl_context)
        if new_pages:
            self.logger.warning(
                f"Found new PacifiCorp queue pages: {new_pages}. "
                "Add them to KNOWN_QUEUE_PAGES."
            )
        pages |= new_pages

        for partition, url in pages.items():
            yield self.get_queue_page(partition, url, ssl_context)

    async def fetch_page(
        self, url: str, ssl_context: ssl.SSLContext
    ) -> tuple[int, bytes]:
        """Fetch a page from the OATI OASIS site, returning its status and contents."""
        response = await retry_async(
            self.session.get, args=[url], kwargs={"ssl": ssl_context}
        )
        body = await retry_async(response.read)
        return response.status, body

    async def find_new_queue_pages(self, ssl_context: ssl.SSLContext) -> dict[str, str]:
        """Check whether PacifiCorp has posted queue pages for any new clusters."""
        found = {}
        for partition, url in CANDIDATE_QUEUE_PAGES.items():
            status, _ = await self.fetch_page(url, ssl_context)
            if status == 200:
                found[partition] = url
            elif status != 404:
                self.logger.warning(f"Unexpected HTTP {status} when checking {url}.")
        return found

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

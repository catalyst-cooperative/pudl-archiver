"""Download MSHA data."""

import logging
import re
import typing
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

logger = logging.getLogger(f"catalystcoop.{__name__}")

BASE_URL = "https://arlweb.msha.gov/OpenGovernmentData/"
BROWSER_EXT = "OGIMSHA.asp"


class MshaArchiver(AbstractDatasetArchiver):
    """MSHA archiver."""

    name = "mshamines"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download MSHA resources."""
        link_pattern = re.compile(r"([a-zA-Z]+).zip")
        for link in await self.get_hyperlinks(BASE_URL + BROWSER_EXT):
            logger.info(link)
        for link in await self.get_hyperlinks(BASE_URL + BROWSER_EXT, link_pattern):
            yield self.get_dataset_resource(link, link_pattern.search(link))

    async def get_dataset_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip file."""
        url = BASE_URL + link
        dataset = match.group(1)
        download_path = self.download_directory / f"msha-{dataset}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"dataset": dataset})

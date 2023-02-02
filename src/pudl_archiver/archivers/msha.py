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

URL_107A = "https://arlweb.msha.gov/OpenGovernmentData/107a/"
EXT_107A = "107aOrders.asp"

datasets = [
    "Accidents",
    "AddressofRecord",
    "AreaSamples",
    "AssessedViolations",
    "CivilPenaltyDocketsDecisions",
    "CoalDustSamples",
    "Conferences",
    "ContestedViolations",
    "ContractorProdQuarterly",
    "ContractorProdYearly",
    "ControllerOperatorHistory",
    "Inspections",
    "Mines",
    "MinesProdQuarterly",
    "MinesProdYearly",
    "NoiseSamples",
    "PersonalHealthSamples",
    "QuartzSamples",
    "Violations",
    "107aOrders",
]
""" List of expected MSHA tables/definition files."""


class MshaArchiver(AbstractDatasetArchiver):
    """MSHA archiver."""

    name = "mshamines"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download MSHA resources."""
        link_pattern = re.compile(
            r"(?:DataSets\/)([a-zA-Z0-7_]+)(.zip|_Definition_File.txt)"
        )
        for link in await self.get_hyperlinks(BASE_URL + BROWSER_EXT, link_pattern):
            yield self.get_dataset_resource(link, link_pattern.search(link))

        link_pattern = re.compile(r"(?:107a\/)([a-zA-Z0-7]+).xlsx")
        for link in await self.get_hyperlinks(URL_107A + EXT_107A, link_pattern):
            yield self.get_107a(link, link_pattern.search(link))

    async def get_dataset_resource(
        self, link: str, match: typing.Match
    ) -> tuple[Path, dict]:
        """Download zip and .txt files."""
        url = BASE_URL + match.string
        dataset = match.group(1)

        """Dataset names do not match between data and definition files,
        so we fix this manually."""
        dataset = dataset[0].upper() + dataset[1:]
        dataset = (
            dataset.replace("_", "")
            .replace("And", "")
            .replace("MineS", "Mines")
            .replace("Annual", "Yearly")
        )
        if dataset.endswith("Sample"):
            dataset = dataset.replace("Sample", "Samples")

        if dataset not in datasets:
            logger.info(f"Dataset {dataset} not in existing dataset partitions.")

        if ".zip" in match.string:
            download_path = self.download_directory / f"msha-{dataset}.zip"
            await self.download_zipfile(url, download_path)

        elif ".txt" in match.string:
            download_path = self.download_directory / f"msha-{dataset}-definitions.txt"
            await self.download_file(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"dataset": dataset})

    async def get_107a(self, link: str, match: typing.Match) -> tuple[Path, dict]:
        """Download .xlsx file."""
        url = URL_107A + match.string
        dataset = match.group(1)

        if dataset not in datasets:
            logger.info(f"Dataset {dataset} not in existing dataset partitions.")

        download_path = self.download_directory / f"msha-{dataset}.xlsx"
        await self.download_file(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"dataset": dataset})

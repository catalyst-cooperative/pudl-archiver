"""Download MSHA data."""

import re
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

URL_BASE = "https://arlweb.msha.gov/OpenGovernmentData/"
EXT_BASE = "OGIMSHA.asp"

MSHA_DATASETS = {
    "accidents": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Accidents.zip",
    "accidents_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Accidents_Definition_File.txt",
    "address_of_record": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/AddressofRecord.zip",
    "address_of_record_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/AddressofRecord_Definition_File.txt",
    "area_samples": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/AreaSamples.zip",
    "area_samples_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Area_Samples_Definition_File.txt",
    "assessed_violations": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/AssessedViolations.zip",
    "assessed_violations_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Assessed_Violations_Definition_File.txt",
    "civil_penalty_dockets_decisions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/CivilPenaltyDocketsDecisions.zip",
    "civil_penalty_dockets_decisions_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Civil_Penalty_Dockets_And_Decisions_Definition_File.txt",
    "coal_dust_samples": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/CoalDustSamples.zip",
    "coal_dust_samples_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Coal_Dust_Sample_Definition_File.txt",
    "conferences": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Conferences.zip",
    "conferences_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Conferences_Definition_File.txt",
    "contested_violations": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ContestedViolations.zip",
    "contested_violations_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Contested_Violations_Definition_File.txt",
    "contractor_prod_quarterly": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ContractorProdQuarterly.zip",
    "contractor_prod_quarterly_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ContractorProdQuarterly_Definition_File.txt",
    "contractor_prod_yearly": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ContractorProdYearly.zip",
    "contractor_prod_yearly_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ContractorProdAnnual_Definition_File.txt",
    "controller_operator_history": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/ControllerOperatorHistory.zip",
    "controller_operator_history_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Controller_Operator_History_Definition_File.txt",
    "inspections": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Inspections.zip",
    "inspections_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Inspections_Definition_File.txt",
    "mines": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip",
    "mines_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines_Definition_File.txt",
    "mines_prod_quarterly": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/MinesProdQuarterly.zip",
    "mines_prod_quarterly_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/MineSProdQuarterly_Definition_File.txt",
    "mines_prod_yearly": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/MinesProdYearly.zip",
    "mines_prod_yearly_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/MineSProdYearly_Definition_File.txt",
    "noise_samples": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/NoiseSamples.zip",
    "noise_samples_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Noise_Samples_Definition_File.txt",
    "personal_health_samples": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/PersonalHealthSamples.zip",
    "personal_health_samples_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Personal_Health_Samples_Definition_File.txt",
    "quartz_samples": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/QuartzSamples.zip",
    "quartz_samples_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Quartz_Samples_Definition_File.txt",
    "violations": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Violations.zip",
    "violations_definitions": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/violations_Definition_File.txt",
    "107a": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/OrdersIssued.zip",
}
"""Dictionary of expected MSHA data and definition files, and corresponding URLs."""


class MshaArchiver(AbstractDatasetArchiver):
    """MSHA archiver."""

    name = "mshamines"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download MSHA resources."""
        link_pattern = re.compile(
            r"(?:DataSets\/)([a-zA-Z0-7_]+)(.zip|_Definition_File.txt)"
        )

        # Get main table links.
        links = await self.get_hyperlinks(URL_BASE + EXT_BASE, link_pattern)
        links = [link.split("/")[-1] for link in links]
        full_links = [URL_BASE + "DataSets/" + link for link in links]

        self.logger.debug(full_links)

        if any(item not in list(set(MSHA_DATASETS.values())) for item in full_links):
            # If a link to a new dataset is found, raise error.
            new_links = " ".join(
                [v for v in full_links if v not in MSHA_DATASETS.values()]
            )
            raise ValueError(f"New dataset download links found: {new_links}")

        if not all(item in full_links for item in list(set(MSHA_DATASETS.values()))):
            # If an expected dataset link is missing, raise error.
            missing_data = ", ".join(
                [k for k, v in MSHA_DATASETS.items() if v not in full_links]
            )
            raise ValueError(
                f"Expected dataset download links not found for datasets: {missing_data}"
            )

        for link in full_links:
            yield self.get_dataset_resource(link)

    async def get_dataset_resource(self, link: str) -> tuple[Path, dict]:
        """Download zip and .txt files."""
        filename = list(MSHA_DATASETS.keys())[list(MSHA_DATASETS.values()).index(link)]
        # Get file name (dictionary key) from dictionary value.
        dataset = filename.replace("_definitions", "")

        if link.endswith(".zip"):
            download_path = self.download_directory / f"{self.name}-{filename}.zip"
            await self.download_zipfile(link, download_path)

        elif link.endswith(".txt"):
            download_path = self.download_directory / f"{self.name}-{filename}.txt"
            await self.download_file(link, download_path)

        elif link.endswith(".xlsx"):
            download_path = self.download_directory / f"{self.name}-{filename}.xlsx"
            await self.download_file(link, download_path)

        else:
            raise ValueError(f"Unexpected file extension: {link}. File not downloaded.")

        return ResourceInfo(local_path=download_path, partitions={"form": dataset})

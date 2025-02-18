"""Download data from the NREL siting lab data."""

import asyncio
import re

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout


class NrelAPIData(BaseModel):
    """Data transfer object from NREL API."""

    class Submission(BaseModel):
        """Metadata about a specific dataset."""

        submission_name: str
        xdr_id: int
        num_resources: int
        file_count: int
        status: str
        # There are a few other fields that we don't parse here
        # e.g., update date formatted in unix timestamps. We could
        # revisit this in the future.

        class Config:  # noqa: D106
            alias_generator = to_camel
            populate_by_name = True

    result: bool
    num_submissions: int
    num_resources: int
    num_files: int
    size_of_files: int
    stati: dict[str, int]
    submissions: list[Submission]

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class NrelSitingArchiver(AbstractDatasetArchiver):
    """NREL Siting Lab Data archiver."""

    name: str = "nrelsiting"
    base_url: str = "https://data.openei.org/siting_lab"
    concurrency_limit = 1  # The server can get a bit cranky, so let's be nice.

    async def get_resources(self) -> ArchiveAwaitable:
        """Using data IDs, iterate and download all NREL Siting Lab files."""
        # The links on the table are hidden through Javascript. However, we can hit
        # the API to get a dictionary containing metadata on each of the datasets
        # associated with the Siting Lab.
        url = "https://data.openei.org/api"
        data = {
            "action": "getSubmissionStatistics",  # Get high-level data about the submissions
            "format": "json",
            "s": "siting_lab",  # The name of the lab's data we want
        }
        data_dict = await self.get_json(url=url, post=True, data=data)
        # This returns a data dictionary containing metadata on
        # the number of submissions, files, the ID (xdrId) of the dataset
        # that corresponds to the Open EI link, the name, description and more.
        data_dict = NrelAPIData(**data_dict)

        self.logger.info(
            f"Downloading data for {data_dict.num_submissions} datasets. {data_dict.num_files} files ({data_dict.size_of_files * 1e-9} GB)."
        )
        for dataset in data_dict.submissions:
            yield self.get_siting_resources(dataset=dataset)

    async def compile_nrel_download_links(
        self, dataset_id: str, dataset_link: str
    ) -> set:
        """For a given NREL dataset link, grab all PDFs and data links from the page."""
        # There are many file types here, so we match using the more general link pattern
        # e.g., https://data.openei.org/files/6121/nexrad_4km.tif
        # We also grab the PDF files, which are hosted on a different part of the
        # NREL website. E.g., https://www.nrel.gov/docs/fy24osti/87843.pdf
        download_links = set()

        data_pattern = re.compile(rf"files\/{dataset_id}\/")
        pdf_data_pattern = re.compile(r"docs\/[\w\/]*.pdf$")

        # Get data
        data_download_links = await self.get_hyperlinks(dataset_link, data_pattern)
        for link in data_download_links:
            full_link = f"https://data.openei.org{link}"
            download_links.add(full_link)

        # Get PDFs
        pdf_download_links = await self.get_hyperlinks(dataset_link, pdf_data_pattern)
        download_links.update(pdf_download_links)
        return download_links

    async def get_siting_resources(self, dataset: NrelAPIData.Submission):
        """Download all files for a siting resource."""
        dataset_id = dataset.xdr_id

        dataset_link = f"https://data.openei.org/submissions/{dataset_id}"

        # Create zipfile name from dataset name
        title = dataset.submission_name
        dataset_name = title.lower().strip()
        dataset_name = re.sub(
            r"([^a-zA-Z0-9 ])", "", dataset_name
        )  # Drop all non-space special characters
        dataset_name = dataset_name.replace(" ", "-")

        zip_path = self.download_directory / f"nrelsiting-{dataset_name}.zip"
        data_paths_in_archive = set()

        # First, get all the links from the page itself
        data_links = await self.compile_nrel_download_links(
            dataset_id=dataset_id, dataset_link=dataset_link
        )

        # A few datasets have an additional linked data page:
        # e.g., https://data.openei.org/submissions/1932
        additional_datasets_pattern = re.compile(r"\/submissions\/\d{4}")
        links = await self.get_hyperlinks(dataset_link, additional_datasets_pattern)

        # For each additional dataset linked, iterate through the same process
        for link in links:
            additional_dataset_id = link.split("/")[-1]
            additional_data_paths_in_archive = await self.compile_nrel_download_links(
                dataset_id=additional_dataset_id, dataset_link=link
            )
            data_links.update(additional_data_paths_in_archive)

        # For each link we've collected, download it and add it to the zipfile
        data_links = set(data_links)  # Use set to handle duplication
        self.logger.info(
            f"{dataset.submission_name}: Downloading {len(data_links)} files associated with {dataset_link}"
        )

        for link in data_links:
            filename = link.split("/")[-1]
            # This file shows up in multiple datasets,
            # causing collision when they run concurrently. Rename it
            # to avoid this problem.
            if filename == "87843.pdf":
                filename = f"{dataset_name}-technical-report.pdf"

            self.logger.info(f"Downloading {link} to {filename} for {zip_path}.")
            await self.download_add_to_archive_and_unlink(
                url=link, filename=filename, zip_path=zip_path
            )
            data_paths_in_archive.add(filename)
            await asyncio.sleep(10)  # Attempt to reduce server throttling

        return ResourceInfo(
            local_path=zip_path,
            partitions={"data_set": dataset_name},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

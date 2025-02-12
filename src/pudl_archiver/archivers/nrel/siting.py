"""Download data from the NREL siting lab data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
    retry_async,
)
from pudl_archiver.frictionless import ZipLayout


class NrelSitingArchiver(AbstractDatasetArchiver):
    """NREL Siting Lab Data archiver."""

    name: str = "nrelsiting"
    base_url: str = "https://data.openei.org/siting_lab"

    async def get_resources(self) -> ArchiveAwaitable:
        """Using data IDs, iterate and download all NREL Siting Lab files."""
        # The links on the table are hidden through Javascript. However,
        # the IDs are exposed on this JS file, which links each dataset ID to an image.
        # Rather than using Selenium, we can use this file to identify the links for all
        # datasets hosted through the siting lab.
        url = "https://data.openei.org/api"
        data = {
            "action": "getSubmissionStatistics",
            "format": "json",
            "s": "siting_lab",
        }
        response = await retry_async(
            self.session.post, args=[url], kwargs={"data": data}
        )
        data_dict = await response.json()

        self.logger.info(
            f"Downloading data for {data_dict['numSubmissions']} datasets. {data_dict['numFiles']} files ({data_dict['sizeOfFiles'] / 1e-9} GB)."
        )
        for item in data_dict["submissions"]:
            yield self.get_siting_resources(item)

    async def download_nrel_data(self, dataset_id: str, dataset_link: str) -> set:
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

    async def get_siting_resources(self, dataset_dict: dict[str, str | int | list]):
        """Download all files for a siting resource."""
        dataset_id = dataset_dict["xdrId"]

        dataset_link = f"https://data.openei.org/submissions/{dataset_id}"
        self.logger.info(f"Downloading files from {dataset_link}")

        # Create zipfile name from dataset name
        title = dataset_dict["submissionName"]
        dataset_name = title.lower().strip()
        dataset_name = re.sub(
            r"([^a-zA-Z0-9 ])", "", dataset_name
        )  # Drop all non-space special characters
        dataset_name = dataset_name.replace(" ", "-")

        zip_path = self.download_directory / f"nrelsiting-{dataset_name}.zip"
        data_paths_in_archive = set()

        # First, get all the links from the page itself
        data_links = await self.download_nrel_data(
            dataset_id=dataset_id, dataset_link=dataset_link
        )

        # A few datasets have an additional linked data page:
        # e.g., https://data.openei.org/submissions/1932
        additional_datasets_pattern = re.compile(r"\/submissions\/\d{4}")
        links = await self.get_hyperlinks(dataset_link, additional_datasets_pattern)

        # For each additional dataset linked, iterate through the same process
        for link in links:
            additional_dataset_id = link.split("/")[-1]
            additional_data_paths_in_archive = await self.download_nrel_data(
                dataset_id=additional_dataset_id, dataset_link=link
            )
            data_links.update(additional_data_paths_in_archive)

        # For each link we've collected, download it and add it to the zipfile
        for link in set(data_links):  # Use set to handle duplication
            filename = link.split("/")[-1]
            # This file shows up in multiple datasets,
            # causing collision when they run concurrently. Rename it
            # to avoid this problem.
            if filename == "87843.pdf":
                filename = f"{dataset_name}-technical-report.pdf"

            self.logger.debug(f"Downloading {link} to {filename} for {zip_path}.")
            await self.download_add_to_archive_and_unlink(
                url=link, filename=filename, zip_path=zip_path
            )
            data_paths_in_archive.add(filename)

        return ResourceInfo(
            local_path=zip_path,
            partitions={"data_set": dataset_name},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

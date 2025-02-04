"""Download NREL Electrification Futures Study data.

The EFS data is composed of 6 reports, each with technical reports, data files
and occasionally other files. The reports are linked on the main page, while data
is contained on other websites that are linked to from the main page. The reports
were released over the span of a few years, and we bundle files associated with
each report into one zip file in this archive.
"""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.validate import ZipLayout

# Main page contains PDF links and links to other pages
BASE_URL = "https://www.nrel.gov/analysis/electrification-futures.html"

# Data is also contained on sites with the following formats
# https://data.nrel.gov/submissions/##
# https://data.openei.org/submissions/##


class NrelEFSArchiver(AbstractDatasetArchiver):
    """NREL Electrification Futures Studies archiver."""

    name = "nrelefs"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL EFS resources.

        The main page links to a series of PDFs as well as data.nrel.gov and data.openei.org webpages
        containing associated data for each report.
        """
        # Hard-code a dictionary of each version of the study, with a short-hand
        # description of the report as the key and the links to all data and reports
        # in the version as the values. This was last published in 2021 so we don't
        # expect these to change.

        version_dict = {
            "cost-and-performance": [
                "https://www.nrel.gov/docs/fy18osti/70485.pdf",
                "https://data.nrel.gov/submissions/93",
                "https://data.nrel.gov/submissions/78",
            ],
            "demand-side-scenarios": [
                "https://www.nrel.gov/docs/fy18osti/71500.pdf",
                "https://www.nrel.gov/docs/fy18osti/72096.pdf",
                "https://www.nrel.gov/docs/fy18osti/72311.pdf",
                "https://data.nrel.gov/submissions/90",
                "https://data.nrel.gov/submissions/92",
            ],
            "dsgrid-model": [
                "https://www.nrel.gov/docs/fy18osti/71492.pdf",
                "https://www.nrel.gov/docs/fy18osti/72388.pdf",
                "https://data.openei.org/submissions/4130",
            ],
            "load-profiles": [
                "https://www.nrel.gov/docs/fy20osti/73336.pdf",
                "https://data.nrel.gov/submissions/126",
                "https://data.nrel.gov/submissions/127",
            ],
            "supply-side-scenarios": [
                "https://www.nrel.gov/docs/fy21osti/72330.pdf",
                "https://www.nrel.gov/docs/fy21osti/78783.pdf",
                "https://data.nrel.gov/submissions/157",
            ],
            "detailed-grid-simulations": [
                "https://www.nrel.gov/docs/fy21osti/79094.pdf",
                "https://www.nrel.gov/docs/fy21osti/80167.pdf",
            ],
        }

        # Though we hardcode the links above, we also grab the PDFs links from the page
        # in order to get the filename associated with the link. This makes it
        # easier to label each PDF something informative
        pdf_pattern = re.compile(r"\/docs\/fy(\d{2})osti\/\w*.pdf")
        pdf_links = await self.get_hyperlinks(BASE_URL, pdf_pattern)

        # For each version, yield a method that will produce one zipfile containing
        # all the files for the method
        for version, links in version_dict.items():
            yield self.get_version_resource(
                version=version, links=links, pdf_links=pdf_links
            )

        # Let us know what links we aren't grabbing.
        links_not_downloaded = [
            f"https://www.nrel.gov{link}"
            for link in pdf_links
            if link not in version_dict
        ]
        self.logger.warn(
            f"Not downloading the following additional PDFs linked from the mainpage: {links_not_downloaded}"
        )

    async def get_version_resource(
        self,
        version: str,
        links: list[str],
        pdf_links: list[dict[str, str]],
    ) -> ResourceInfo:
        """Download all available data for a given version of an EFS study.

        Resulting resource contains one zip file of all PDFs, .zip, .xlsx, .gzip, and
        .csv.gzip files for a given version of the EFS studies. We handle the DS Grid
        specially because the data is hosted on an S3 viewer with a nested file structure.

        Args:
            version: shorthand name for the given version
            links: a list of links that contain data for this version.
            pdf_links: a list of all PDF links found on the EFS homepage, with the title
                of the link. We use this to rename the PDFs to something more informative
                than the original file title.
        """
        # Set up zipfile name and list of files in zip
        zipfile_path = self.download_directory / f"nrelefs-{version}.zip"
        data_paths_in_archive = set()

        # Compile pattern for all datasets on data.nrel.gov
        data_pattern = re.compile(
            r"files\/([\w\/]*)\/([\w \-%]*)(.zip|.xlsx|.gzip|.csv.gzip)$"
        )

        # Compile list of sub-folders and and regex pattern for DSGrid special case
        dsgrid_list = [
            "dsgrid-site-energy-state-hourly",
            "raw-complete",
            "state-hourly-residuals",
        ]
        dsg_pattern = re.compile(r"[\w]*.dsg$")

        for link in links:
            # First, get all the PDFs
            if link.endswith(".pdf"):
                matching_pdf_link = [key for key in pdf_links if key in link]
                # Get the corresponding filename from pdf_links
                if matching_pdf_link:
                    link_key = matching_pdf_link.pop()
                    filename = pdf_links[link_key]  # TODO: Debug this
                    # Clean the filename to name the PDF something more informative than
                    # the link name
                    self.logger.info(f"Downloading {link}")
                    filename = (
                        filename.lower()
                        .replace("\n", "")
                        .replace("electrification futures study:", "")
                    )
                    filename = re.sub(
                        "[^a-zA-Z0-9 -]+", "", filename
                    ).strip()  # Remove all non-word, digit space or - characters
                    filename = re.sub(
                        r"\s+", "-", filename
                    )  # Replace 1+ space with a dash
                    filename = f"nrelefs-{version}-{filename}.pdf"
                    await self.download_add_to_archive_and_unlink(
                        url=link, filename=filename, zip_path=zipfile_path
                    )
                    data_paths_in_archive.add(filename)
                else:
                    # Alert us to expected but missing PDF links.
                    raise AssertionError(
                        f"Expected PDF link {link} but this wasn't found in {BASE_URL}. Has the home page changed?"
                    )

            # Next, get all the data files from data.nrel.gov
            elif "data.nrel.gov/submissions/" in link:
                self.logger.info(f"Downloading data files from {link}.")
                data_links = await self.get_hyperlinks(link, data_pattern)
                for data_link, filename in data_links.items():
                    matches = data_pattern.search(data_link)
                    if not matches:
                        continue
                    # Grab file name and extension
                    filename = matches.group(2)
                    file_ext = matches.group(3)

                    # Reformat filename
                    filename = filename.lower().replace("_", "-").replace("%20", "-")
                    filename = re.sub(
                        "[^a-zA-Z0-9 -]+", "", filename
                    ).strip()  # Remove all non-word, digit space or - characters
                    filename = re.sub(r"[\s-]+", "-", filename)
                    filename = re.sub(
                        r"^efs-", "", filename
                    )  # We add this back with an nrel header
                    filename = f"nrelefs-{version}-{filename}{file_ext}"
                    self.logger.info(
                        f"Downloading {data_link} as {filename} to {zipfile_path}."
                    )
                    await self.download_add_to_archive_and_unlink(
                        url=data_link, filename=filename, zip_path=zipfile_path
                    )
                    data_paths_in_archive.add(filename)

            elif "data.openei.org" in link:  # Finally, handle DSGrid data
                self.logger.info("Downloading DSGrid data files.")
                # Iterate through each type of DSGrid data and download
                for data_type in dsgrid_list:
                    # Construct download link from data type
                    dsg_link = f"https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2F{data_type.replace('-', '_')}%2F"
                    dsg_file_links = await self.get_hyperlinks(dsg_link, dsg_pattern)
                    for dsg_link, filename in dsg_file_links.items():
                        filename = filename.replace("_", "-")
                        filename = f"nrelesg-{data_type}-{filename}"
                        await self.download_add_to_archive_and_unlink(
                            url=dsg_link, filename=filename, zip_path=zipfile_path
                        )
                        data_paths_in_archive.add(filename)

            else:
                # Raise error for mysterious other links in dictionary.
                raise AssertionError(f"Unexpected format for link {link} in {version}.")

        return ResourceInfo(
            local_path=zipfile_path,
            partitions={"version": version},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

"""Download NREL Electrification Futures Study data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

# Main page
# https://www.nrel.gov/analysis/electrification-futures.html

# Grab all data sites with the following formats
# https://data.nrel.gov/submissions/90
# https://data.openei.org/submissions/4130

# Also grab all PDFs on the main page
BASE_URL = "https://www.nrel.gov/analysis/electrification-futures.html"


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
        # in order to get information about the name ascribed to the link to make it
        # easier to label each PDF something informative
        pdf_pattern = re.compile(r"\/docs\/fy(\d{2})osti\/\w*.pdf")
        pdf_links = await self.get_hyperlinks(BASE_URL, pdf_pattern)

        # For each version, yield a method that will produce one zipfile containing
        # all the files for the method
        for version, links in version_dict.items():
            yield get_version_resource(
                version=version, links=links, pdf_links=pdf_links
            )

        data_link_pattern = re.compile(r"data.nrel.gov\/submissions\/")
        # Regex for matching the page containing data for a study on the NREL EFS page.

        # Regex for matching a PDF on the page

        # From the main page, grab all the PDFs

        for link, filename in pdf_links.items():
            # Flow through workflow to identify the version of the PDF,
            # the final filename

            # Clean up file name
            self.logger.info(f"Downloading {link}")
            filename = (
                filename.lower()
                .replace("\n", "")
                .replace("electrification futures study:", "")
            )
            filename = re.sub(
                "[^a-zA-Z0-9 -]+", "", filename
            ).strip()  # Remove all non-word, digit space or - characters
            filename = re.sub(r"\s+", "-", filename)  # Replace 1+ space with a dash

            # Map technical reports to versions
            technical_report_version_map = {
                "operational-analysis-of-us-power-systems-with-increased-electrification-and-demand-side-flexibility": 6,
                "scenarios-of-power-system-evolution-and-infrastructure-development-for-the-united-states": 5,
                "methodological-approaches-for-assessing-long-term-power-system-impacts-of-end-use-electrificatio": 4,
                "the-demand-side-grid-dsgrid-model-documentation": 3,
                "scenarios-of-electric-technology-adoption-and-power-consumption-for-the-united-states": 2,
                "end-use-electric-technology-cost-and-performance-projections-through-2050": 1,
            }

            if filename in technical_report_version_map:
                final_filename = f"nrelefs-{filename}.pdf"
                partitions = {
                    "report_number": technical_report_version_map[filename],
                    "document_type": "technical_report",
                }

            # Map "presentation slides" to version based on URL
            elif filename == "presentation-slides":
                link_to_version = {
                    "/docs/fy21osti/80167.pdf": 6,
                    "/docs/fy21osti/78783.pdf": 5,
                    "/docs/fy18osti/72096.pdf": 2,
                }

                report_number = link_to_version[link]
                final_filename = f"nrelefs-{str(report_number)}-{filename}.pdf"
                partitions = {
                    "report_number": report_number,
                    "document_type": "presentation",
                }

            # Handle 2 special cases
            elif (
                filename
                == "electrification-of-industry-summary-of-efs-industrial-sector-analysis"
            ):
                final_filename = f"nrelefs-{filename}.pdf"
                partitions = {
                    "report_number": 2,
                    "document_type": "industrial_sector_presentation",
                }

            elif filename == "the-demand-side-grid-dsgrid-model":
                final_filename = f"nrelefs-{filename}.pdf"
                partitions = {"report_number": 3, "document_type": "presentation"}

            # Ignore a few other PDF links on the page that aren't from the EFS
            else:
                self.logger.warn(f"Found {filename} at {link} but didn't download.")
                continue
            yield self.get_pdf_resource(final_filename, link, partitions)

        # For each data link found on the page, iterate through and download files
        for link in await self.get_hyperlinks(BASE_URL, data_link_pattern):
            data_pattern = re.compile(
                r"files\/([\w\/]*)\/([\w \-%]*)(.zip|.xlsx|.gzip|.csv.gzip)$"
            )
            for data_link in await self.get_hyperlinks(link, data_pattern):
                matches = data_pattern.search(data_link)
                if not matches:
                    continue
                yield self.get_version_resource(data_link=data_link, matches=matches)

        # Finally, get data from the DSGrid Data Lake
        # Zip each
        dsgrid_dict = {
            "dsgrid-site-energy-state-hourly": "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2Fdsgrid_site_energy_state_hourly%2F",
            "raw-complete": "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2Fraw_complete%2F",
            "state-hourly-residuals": "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2Fstate_hourly_residuals%2F",
        }
        for zip_filename, link in dsgrid_dict.items():
            yield self.get_dsgrid_resource(zip_filename=zip_filename, link=link)

    async def get_version_resource(
        self,
        data_link: str,
        matches: re.Match,
    ) -> ResourceInfo:
        """Download all available data for a given page of EFS data.

        Resulting resource contains one zip file of CSVs per state/territory, plus a handful of .xlsx dictionary and geocoding files.

        Args:
            links: filename->URL mapping for files to download
        """
        # Grab file name and extension
        filename = matches.group(2)
        file_ext = matches.group(3)

        # Reformat filename
        filename = filename.lower().replace("_", "-")
        filename = re.sub(
            "[^a-zA-Z0-9 -]+", "", filename
        ).strip()  # Remove all non-word, digit space or - characters
        filename = re.sub(r"[\s-]+", "-", filename)
        filename = re.sub(
            r"^efs-", "", filename
        )  # We add this back with an nrel header

        download_path = self.download_directory / f"nrelefs-{filename}{file_ext}"

        if file_ext == ".zip" or ".gzip" in file_ext:
            await self.download_zipfile(url=data_link, zip_path=download_path)

        else:
            await self.download_file(url=data_link, file_path=download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={
                "document_type": "data",
                "data_file": filename,
                "report_number": 0,
            },  # TO DO!
        )

    async def get_pdf_resource(
        self, final_filename: str, link: str, partitions: dict[str, str | int]
    ) -> ResourceInfo:
        """Download PDF resource.

        Resulting resource contains one PDF file with information about the EFS dataset.

        Args:
            link: filename->URL mapping for files to download
            filename: the name of the file on the NREL EFS webpage
            partitions: partitions for downloaded file
        """
        download_path = self.download_directory / final_filename
        full_link = f"https://www.nrel.gov/{link}"
        await self.download_file(url=full_link, file_path=download_path)
        return ResourceInfo(
            local_path=download_path,
            partitions=partitions,
        )

    async def get_dsgrid_resource(self, zip_filename: str, link: str) -> ResourceInfo:
        """Download DSGRID resources into one zipped file.

        Resulting resource contains many .dgrid files in one zip file.

        Args:
            filename: the name of the final zipfile
            link: URL where the files to download are found
        """
        data_paths_in_archive = set()
        zipfile_path = self.download_directory / f"nrelefs-{zip_filename}.zip"
        dsg_pattern = re.compile(r"[\w]*.dsg$")
        dsg_file_links = await self.get_hyperlinks(link, dsg_pattern)
        for link, filename in dsg_file_links.items():
            await self.download_add_to_archive_and_unlink(
                url=link, filename=filename, zip_path=zipfile_path
            )
            data_paths_in_archive.add(filename)
        return ResourceInfo(
            local_path=zipfile_path,
            partitions={"document_type": "data", "data_file": zip_filename},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

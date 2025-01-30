"""Download NREL Electrification Futures Study data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

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
        data_link_pattern = re.compile(
            r"https:\/\/data.nrel.gov\/submissions\/|https:\/\/data.openei.org\/submissions\/"
        )
        # Regex for matching the two pages containing data for a study on the NREL EFS page.

        pdf_pattern = re.compile(r"\/docs\/fy(\d{2})osti\/\w*.pdf")
        # Regex for matching a PDF on the page

        # From the main page, grab all the PDFs
        pdf_links = await self.get_hyperlinks(BASE_URL, pdf_pattern)
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
            yield self.get_version_resource(link=link)

    async def get_version_resource(
        self, links: dict[str, str], year: int
    ) -> ResourceInfo:
        """Download all available data for a given page of EFS data.

        Resulting resource contains one zip file of CSVs per state/territory, plus a handful of .xlsx dictionary and geocoding files.

        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
        # host = "https://data.openei.org"
        # zip_path = self.download_directory / f"doelead-{year}.zip"
        # data_paths_in_archive = set()
        # for filename, link in sorted(links.items()):
        #     self.logger.info(f"Downloading {link}")
        #     download_path = self.download_directory / filename
        #     await self.download_file(f"{host}{link}", download_path)
        #     self.add_to_archive(
        #         zip_path=zip_path,
        #         filename=filename,
        #         blob=download_path.open("rb"),
        #     )
        #     data_paths_in_archive.add(filename)
        #     # Don't want to leave multiple giant files on disk, so delete
        #     # immediately after they're safely stored in the ZIP
        #     download_path.unlink()
        # return ResourceInfo(
        #     local_path=zip_path,
        #     partitions={"year": year},
        #     layout=ZipLayout(file_paths=data_paths_in_archive),
        # )

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

"""Download NREL Standard Scenarios data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.utils import retry_async

API_URL_PROJECTS_LIST = "https://scenarioviewer.nrel.gov/api/projects/"
API_URL_FILE_LIST = "https://scenarioviewer.nrel.gov/api/file-list/"
API_URL_FILE_DOWNLOAD = "https://scenarioviewer.nrel.gov/api/download/"

REPORT_URL_PATTERN = re.compile(
    r"https://www.nrel.gov/docs/(?P<fy>fy\d{2}osti)/(?P<number>\d{5}\.pdf)"
)


class AbstractNrelScenarioArchiver(AbstractDatasetArchiver):
    """Base class for archiving projects from the NREL Scenario Viewer."""

    project_year_pattern: re.Pattern
    project_startswith: str
    report_section: str
    file_naming_order: tuple[str]

    def handle_report_link_exceptions(self, project_year: str) -> str:
        """Provide hard-coded exceptions for years where the normal report URL locator doesn't work."""
        return ""

    async def collect_project_info(
        self, scenario_project: dict
    ) -> tuple[str, int, list, list]:
        """Gather all the information needed to download a single project.

        Args:
            scenario_project: dictionary representing a single project, as extracted from /api/projects/ JSON

        Returns:
            Tuple of:
                - Project UUID
                - Project year
                - List of (filename, url) pairs for PDF reports
                - List of (filename, id) pairs for files
        """
        project_uuid = scenario_project["uuid"]
        m = self.project_year_pattern.search(scenario_project["name"])
        if not m:
            return (False,) * 3
        project_year = int(m.group("year"))

        report_links = self.get_hyperlinks_from_text(
            scenario_project[self.report_section], REPORT_URL_PATTERN
        )
        if not report_links:
            report_links = {self.handle_report_link_exceptions(project_year)}
        if not report_links:
            raise AssertionError(
                f"We expect all projects to have a {self.report_section} with a link to the report PDF, but {project_year} does not:\n"
                f"{scenario_project}"
            )
        report_data = []
        for report_link in report_links:
            m = REPORT_URL_PATTERN.search(report_link)
            if not m:
                raise AssertionError(
                    f"Bad report link {report_link} found in {scenario_project['name']} {project_uuid}: {scenario_project}"
                )
            report_data.append((f"{m.group('fy')}_{m.group('number')}", report_link))

        file_list_resp = await retry_async(
            self.session.post,
            [API_URL_FILE_LIST],
            kwargs={"data": {"project_uuid": project_uuid}},
        )
        file_list = await retry_async(file_list_resp.json)

        return (
            project_uuid,
            project_year,
            report_data,
            [
                (
                    f"{self.name.upper()} {project_year}  {'  '.join(f[x] for x in self.file_naming_order)}.{f['file_type']}".replace(
                        " ", "_"
                    )
                    .replace("%", "pct")
                    .replace(",", "")
                    .lower(),
                    f["id"],
                )
                for f in file_list["files"]
            ],
        )

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Scenario resources.

        Basic flow:
            1. Fetch the list of projects. The scenario viewer includes Standard scenarios, Cambium scenarios, and a few other types, so we filter down using the project name.
            2. Pull out metadata for matching projects: uuid, year, and links to any PDF reports. PDF report URLs are not provided in a dedicated field in the project response, but are part of an HTML value for the description or citation in the project. Sometimes this field is simply blank, and we need to use a hard-coded exception.
            3. Fetch the list of data files for the project, and construct a well-ordered filename for each.
            4. Download and package all files for the project, and yield its ResourceInfo.
        """
        project_records = await self.get_json(API_URL_PROJECTS_LIST)
        for scenario_project in (
            p for p in project_records if p["name"].startswith(self.project_startswith)
        ):
            (
                project_uuid,
                project_year,
                report_data,
                file_ids,
            ) = await self.collect_project_info(scenario_project)
            if project_uuid:
                yield self.get_project_resource(
                    uuid=project_uuid,
                    year=project_year,
                    reports=report_data,
                    file_ids=file_ids,
                )

    async def get_project_resource(
        self,
        uuid: str,
        year: int,
        reports: list[tuple[str, str]],
        file_ids: list[tuple[str, str]],
    ) -> ResourceInfo:
        """Download all available data for a project.

        Resulting resource contains PDFs of the scenario report(s), and a set of CSVs or ZIPs for different slices.

        Args:
            uuid: identifier for the project
            year: the year of the project
            reports: list of (filename, url) pairs for PDF scenario reports included with the project
            file_ids: list of (filename, id) pairs for files in the project
        """
        zip_path = self.download_directory / f"{self.name}-{year}.zip"

        # reports: direct URL
        for filename, url in reports:
            self.logger.info(f"Downloading report {year} {filename} {uuid} from {url}")
            self.download_add_to_archive_and_unlink(url, filename, zip_path)

        # files: API call
        for filename, file_id in file_ids:
            self.logger.info(f"Downloading file {year} {file_id} {uuid}")
            download_path = self.download_directory / filename
            await self.download_file(
                API_URL_FILE_DOWNLOAD,
                download_path,
                data={"project_uuid": uuid, "file_ids": file_id},
            )
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=download_path.open("rb"),
            )
            # Don't want to leave multiple giant files on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()

        return ResourceInfo(
            local_path=zip_path,
            partitions={"years": year},
        )


class NrelStandardScenariosArchiver(AbstractNrelScenarioArchiver):
    """NREL Standard Scenarios archiver."""

    name = "nrelss"
    project_year_pattern = re.compile(r"Standard Scenarios (?P<year>\d{4})")
    project_startswith = "Standard Scenarios"
    report_section = "citation"
    file_naming_order = ("scenario", "location_type")

    def handle_report_link_exceptions(self, project_year):
        """Hard-coded exception for project year 2021."""
        if project_year == 2021:
            # The citation field for Standard Scenarios 2021 is blank, but they linked to the
            # 2021 report from the description of one of the other available projects, so we're
            # able to hard-code it for now:
            return "https://www.nrel.gov/docs/fy22osti/80641.pdf"
        return ""

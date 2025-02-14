"""Download NREL Cambium Scenarios data."""

import re

from pudl_archiver.archivers.classes import (
    ArchiveAwaitable,
    ResourceInfo,
    _download_file,
)
from pudl_archiver.archivers.nrelss import (
    API_URL_FILE_DOWNLOAD,
    API_URL_PROJECTS_LIST,
    AbstractNrelScenarioArchiver,
)
from pudl_archiver.utils import retry_async


class AbstractNrelCambiumArchiver(AbstractNrelScenarioArchiver):
    """Base class for NREL Cambium archivers."""

    project_year: int
    project_year_pattern = re.compile(r"Cambium (?P<year>\d{4})")
    project_startswith = "Cambium "
    report_section = "long_description"
    file_naming_order = ("scenario", "metric", "time_resolution", "location_type")

    concurrency_limit = 1  # Cambium scenarios are Large so only handle 2 at a time

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Cambium resources.

        Basic flow:
            1. Fetch the list of projects and extract just the one for this archiver.
            2. Pull out metadata: uuid, year, links to any PDF reports, and data files. PDF report URLs are not provided in a dedicated field in the project response, but are part of an HTML value for the description or citation in the project. Sometimes this field is simply blank, and we need to use a hard-coded exception. The data files don't have good filenames associated with them, so we make one.
            4. Download each report and file for the project as separate resources.
        """
        project_records = await self.get_json(API_URL_PROJECTS_LIST)
        scenario_project = [
            p
            for p in project_records
            if p["name"].startswith(f"{self.project_startswith}{self.project_year}")
        ]
        assert len(scenario_project) == 1
        scenario_project = scenario_project.pop()
        (
            project_uuid,
            project_year,
            report_data,
            file_ids,
        ) = await self.collect_project_info(scenario_project)
        assert project_uuid
        for filename, url in report_data:
            yield self.get_report_resource(filename, url)
        for filename, file_id in file_ids:
            yield self.get_file_resource(filename, project_uuid, file_id)

    async def get_report_resource(self, filename, url) -> ResourceInfo:
        """Retrieve and compress PDF report and return as ResourceInfo."""
        self.logger.info(f"Downloading report {filename}")
        zip_path = self.download_directory / f"{filename}.zip"
        await self.download_and_zip_file(url, filename, zip_path)
        return ResourceInfo(
            local_path=zip_path,
            partitions={},
        )

    async def get_file_resource(self, filename, uuid, file_id) -> ResourceInfo:
        """Retrieve and data file and return as ResourceInfo."""
        self.logger.info(f"Downloading file {filename} {file_id} {uuid}")
        download_path = self.download_directory / filename

        await retry_async(
            _download_file,
            [self.session, API_URL_FILE_DOWNLOAD, download_path, True],
            kwargs={"data": {"project_uuid": uuid, "file_ids": file_id}},
            retry_base_s=20,
        )
        return ResourceInfo(
            local_path=download_path,
            partitions={},
        )


class NrelCambium2020Archiver(AbstractNrelCambiumArchiver):
    """NREL Cambium archiver for 2020."""

    name = "nrelcambium2020"
    project_year = 2020


class NrelCambium2021Archiver(AbstractNrelCambiumArchiver):
    """NREL Cambium archiver for 2021."""

    name = "nrelcambium2021"
    project_year = 2021


class NrelCambium2022Archiver(AbstractNrelCambiumArchiver):
    """NREL Cambium archiver for 2022."""

    name = "nrelcambium2022"
    project_year = 2022


class NrelCambium2023Archiver(AbstractNrelCambiumArchiver):
    """NREL Cambium archiver for 2023."""

    name = "nrelcambium2023"
    project_year = 2023

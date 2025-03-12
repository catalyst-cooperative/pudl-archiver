"""Download NREL Cambium Scenarios data."""

import re
from collections import defaultdict

from pudl_archiver.archivers.classes import (
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.archivers.nrel.nrelss import (
    API_URL_FILE_DOWNLOAD,
    API_URL_PROJECTS_LIST,
    AbstractNrelScenarioArchiver,
)
from pudl_archiver.utils import retry_async


class AbstractNrelCambiumArchiver(AbstractNrelScenarioArchiver):
    """Base class for NREL Cambium archivers.

    A new Cambium report/project is published each year. These can vary wildly in size and
    complexity, from 25 to 170 files, and from 7GB to 37GB.

    Because Cambium datasets can be so large, we cannot include all of them in a single
    Zenodo deposition as we normally would, with each new version including the cumulative
    set of all reports. Because Cambium reports differ so much from year to year, it is
    conceivable that someone would want to access multiple years' reports in a single
    analysis, and thus we do not treat the individual reports as different versions of a
    single Zenodo record/archive, either. Instead, each report gets its own record/archive
    on Zenodo, with only one version. Cambium reports are not [expected to be] updated
    after initial publication.

    This base class specifies the behavior for extracting and downloading a single Cambium
    report/project. Subclasses then need specify only the `project_year` and `name`.

    To fit within the max 100 files per record constraint in Zenodo, we partition by
    scenario. The Cambium 2022 Zenodo record would otherwise contain 172 files. Cambium
    reports typically include 5-10 scenarios, with each scenario totaling 70MB-7GB.
    """

    project_year: int
    """The year to download."""

    # specified by AbstractNrelScenarioArchiver
    project_year_pattern = re.compile(r"Cambium (?P<year>\d{4})")
    project_startswith = "Cambium"
    report_section = "long_description"
    file_naming_order = ("scenario", "metric", "time_resolution", "location_type")

    # Cambium files can be up to 3GB and the server is cranky so only handle 1 at a time
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Cambium resources.

        Basic flow:
            1. Fetch the list of projects, and keep only the one for this archiver.
            2. Pull out metadata for matching projects: uuid, year, links to any PDF
               reports, and data files.
            3. Group data files by scenario, with each scenario a partition.
            4. Download and package each report and partition for the project, and yield
               its ResourceInfo.
        """
        project_records = await self.get_json(API_URL_PROJECTS_LIST)
        scenario_project = [
            p
            for p in project_records
            if p["name"].startswith(f"{self.project_startswith} {self.project_year}")
        ]
        assert len(scenario_project) == 1
        scenario_project = scenario_project.pop()
        (
            project_uuid,
            project_year,
            report_data,
            file_ids,
        ) = await self.collect_project_info(scenario_project)
        for filename, url in report_data:
            yield self.get_report_resource(filename, url)
        file_ids_by_partition = defaultdict(list)
        for filename, file_id in file_ids:
            # This is a little brittle, but it's the easiest way to get at the scenario
            # without threading additional outputs through AbstractNrelScenarioArchiver
            # or replicating the scenario name reprocessing therein.
            partition = filename.split("__")[1]
            file_ids_by_partition[partition].append((filename, file_id))
        for partition, partition_file_ids in file_ids_by_partition.items():
            yield self.get_partition_resource(
                project_year, project_uuid, partition, partition_file_ids
            )

    async def get_report_resource(self, filename, url) -> ResourceInfo:
        """Retrieve and compress PDF report and return as ResourceInfo."""
        self.logger.info(f"Downloading report {filename}")
        zip_path = self.download_directory / f"{filename}.zip"
        await self.download_and_zip_file(url, filename, zip_path)
        return ResourceInfo(
            local_path=zip_path,
            partitions={},
        )

    async def get_partition_resource(
        self, year, uuid, partition, partition_file_ids
    ) -> ResourceInfo:
        """Retrieve and data file and return as ResourceInfo.

        The file download API generates a 302 Redirect response that includes an S3 link
        which is somewhat more reliable than the NREL API servers, so we dig that out by
        hand and use it for our retries directly.

        Unfortunately the S3 link expires (!) after two hours, so if we get a 403, we'll
        have saved an xml file (containing an error message) instead of the zip file we
        expect. When that happens, we have to go fetch a new S3 link and try again.
        """
        zip_path = self.download_directory / f"{self.name}_{partition}.zip"
        for i, (filename, file_id) in enumerate(partition_file_ids):
            download_path = self.download_directory / filename

            for s3url_tries in range(5):
                self.logger.info(
                    f"Downloading file {i + 1} of {len(partition_file_ids)} {filename} {file_id} {uuid}{('; S3 link refresh #' + str(s3url_tries)) if s3url_tries > 0 else ''}"
                )

                # aiohttp.session.post somehow does not permit you to specify
                # allow_redirects=False so we gotta use aiohttp.session.request instead
                redirect = await retry_async(
                    self.session.request,
                    kwargs={
                        "method": "POST",
                        "url": API_URL_FILE_DOWNLOAD,
                        "allow_redirects": False,
                        "data": {
                            "file_ids": file_id,
                            "project_uuid": uuid,
                        },
                    },
                )
                assert redirect.status == 302, (
                    f"Something is fishy with the NREL API: received {redirect.status} instead of the expected 302"
                )

                status = await self.download_file(
                    redirect.headers["Location"],
                    download_path,
                )
                if status == 200:
                    # we got a good file; break out of the loop and add it to the archive
                    break
            else:
                # if we run out of tries without getting a good file
                raise AssertionError(
                    f"Timed out the S3 link too many times: {filename} {file_id} {uuid} download failed."
                )
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=download_path.open("rb"),
            )
            # Don't want to leave multiple files on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()

        # While these are all .zip files, we can't use ZipLayout here because the CSVs
        # inside have a multi-row header and we don't yet have a way to tell pandas about
        # it, so validation would fail.
        return ResourceInfo(
            local_path=zip_path,
            partitions={"scenarios": partition},
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

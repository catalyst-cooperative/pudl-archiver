"""Download NREL Standard Scenarios data."""

import re

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import retry_async

# The citation field for Standard Scenarios 2021 is blank, but they linked to the
# 2021 report from the description of one of the other available projects, so we're
# able to hard-code it for now:
REPORT_2021 = "https://www.nrel.gov/docs/fy22osti/80641.pdf"


class NrelStandardScenariosArchiver(AbstractDatasetArchiver):
    """NREL Standard Scenarios archiver."""

    name = "nrelss"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Standard Scenarios resources."""

        async def post_to_json(url, **kwargs):
            resp = await retry_async(self.session.post, [url], data=kwargs)
            return await retry_async(resp.json)

        project_year_pattern = re.compile(r"Standard Scenarios (?P<year>\d{4})")
        report_url_pattern = re.compile(
            r"http://www.nrel.gov/docs/(?P<fy>fy\d{2}osti)/(?P<number>\d{5}\.pdf)"
        )
        filename_pattern = re.compile(r"/([^/?]*/.csv)")

        project_records = self.get_json("https://scenarioviewer.nrel.gov/api/projects/")
        for scenario_project in (
            p for p in project_records if p["name"].startswith("Standard Scenarios")
        ):
            project_uuid = scenario_project["uuid"]
            m = project_year_pattern.search(scenario_project["name"])
            if not m:
                continue
            project_year = int(m.group("year"))

            if scenario_project["citation"]:
                report_link = self.get_hyperlinks_from_text(
                    scenario_project["citation"], report_url_pattern
                ).pop()
            elif project_year == 2021:
                report_link = REPORT_2021
            m = report_url_pattern.search(report_link)
            if not m:
                raise AssertionError(
                    f"We expect all years except 2021 to have a citation with a link to the report, but {project_year} does not:"
                    f"{scenario_project}"
                )
            download_links = {f"{m.group('fy')}_{m.group('number')}": report_link}
            file_list = post_to_json(
                "https://scenarioviewer.nrel.gov/api/file-list/",
                project_uuid=project_uuid,
            )
            for file_record in (
                f for f in file_list["files"] if f["file_type"] == "CSV"
            ):
                file_resp = await retry_async(
                    self.session.post,
                    ["https://scenarioviewer.nrel.gov/api/download/"],
                    data={"project_uuid": project_uuid, "file_ids": file_record["id"]},
                )
                file_headers = file_resp.headers()
                download_filename = f"{file_record['location_type']}.csv"

                m = filename_pattern.search(file_headers["Location"])
                if m:
                    download_filename = m.groups(1)
                else:
                    # this will give us e.g.
                    # (for 2023-2024) "ALL Transmission Capacities.csv" "ALL States.csv"
                    # (for previous years) "Electrification Nations.csv" "High Natural Gas Prices States.csv"
                    download_filename = (
                        f"{file_record['scenario']} {file_record['location_type']}.csv"
                    )

                download_links[download_filename] = file_headers["Location"]
            yield self.get_year_resource(download_links, project_year)

    async def get_year_resource(self, links: dict[str, str], year: int) -> ResourceInfo:
        """Download all available data for a year.

        Resulting resource contains one pdf of the scenario report, and a set of CSVs for different scenarios and geo levels.

        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
        zip_path = self.download_directory / f"{self.name}-{year}.zip"
        data_paths_in_archive = set()
        for filename, link in sorted(links.items()):
            self.logger.info(f"Downloading {filename} from {link}")
            download_path = self.download_directory / filename
            await self.download_file(link, download_path)
            self.add_to_archive(
                zip_path=zip_path,
                filename=filename,
                blob=download_path.open("rb"),
            )
            data_paths_in_archive.add(filename)
            # Don't want to leave multiple giant files on disk, so delete
            # immediately after they're safely stored in the ZIP
            download_path.unlink()
        return ResourceInfo(
            local_path=zip_path,
            partitions={"years": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

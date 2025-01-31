"""Download NREL Standard Scenarios data."""

import aiohttp
from contextlib import nullcontext
import io
from pathlib import Path
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

async def _download_file_post(
    session: aiohttp.ClientSession, url: str, file: Path | io.BytesIO, **kwargs
):
    async with session.post(url, **kwargs) as response:
        with file.open("wb") if isinstance(file, Path) else nullcontext(file) as f:
            async for chunk in response.content.iter_chunked(1024):
                f.write(chunk)

class NrelStandardScenariosArchiver(AbstractDatasetArchiver):
    """NREL Standard Scenarios archiver."""

    name = "nrelss"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Standard Scenarios resources."""

        async def post_to_json(url, **kwargs):
            resp = await retry_async(self.session.post, [url], kwargs={"data":kwargs})
            return await retry_async(resp.json)

        project_year_pattern = re.compile(r"Standard Scenarios (?P<year>\d{4})")
        report_url_pattern = re.compile(
            r"https://www.nrel.gov/docs/(?P<fy>fy\d{2}osti)/(?P<number>\d{5}\.pdf)"
        )
        filename_pattern = re.compile(r"/([^/?]*/.csv)")

        project_records = await self.get_json("https://scenarioviewer.nrel.gov/api/projects/")
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
                )
                if report_link:
                    report_link = report_link.pop()
                else:
                    raise AssertionError(
                        f"We expect all years except 2021 to have a citation with a link to the report, but {project_year} does not:"
                        f"{scenario_project}"
                    )
            elif project_year == 2021:
                report_link = REPORT_2021
            m = report_url_pattern.search(report_link)
            if not m:
                raise AssertionError(
                    f"We expect all years except 2021 to have a citation with a link to the report, but {project_year} does not:"
                    f"{scenario_project}"
                )
                    
            file_list = await post_to_json(
                "https://scenarioviewer.nrel.gov/api/file-list/",
                project_uuid=project_uuid,
            )
            # for file_record in (
#                 
#             ):
#                 file_resp = await retry_async(
#                     self.session.post,
#                     ["https://scenarioviewer.nrel.gov/api/download/"],
#                     kwargs={
#                         "data":{"project_uuid": project_uuid, "file_ids": file_record["id"]},
#                         "kwargs":{"allow_redirects":False}},
#                 )
#                 file_headers = file_resp.headers
#                 download_filename = f"{file_record['location_type']}.csv"
# 
#                 if "Location" not in file_headers:
#                     for h in file_headers:
#                         print(f"{h}: {file_headers[h]}")
#                 m = filename_pattern.search(file_headers["Location"])
#                 if m:
#                     download_filename = m.groups(1)
#                 else:
#                     # this will give us e.g.
#                     # (for 2023-2024) "ALL Transmission Capacities.csv" "ALL States.csv"
#                     # (for previous years) "Electrification Nations.csv" "High Natural Gas Prices States.csv"
#                     download_filename = (
#                         f"{file_record['scenario']} {file_record['location_type']}.csv"
#                     )
# 
#                 download_links[download_filename] = file_headers["Location"]
            yield self.get_year_resource(
                report=(f"{m.group('fy')}_{m.group('number')}", report_link),
                uuid=project_uuid,
                file_ids=[
                    (f["id"], f"NRELSS {project_year}  {f['scenario']}  {f['location_type']}.csv".replace(" ","_"))
                    for f in file_list["files"] if f["file_type"] == "CSV"
                ],
                year=project_year
            )

    async def get_year_resource(self, report, uuid, file_ids, year: int) -> ResourceInfo:
        """Download all available data for a year.

        Resulting resource contains one pdf of the scenario report, and a set of CSVs for different scenarios and geo levels.

        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
        zip_path = self.download_directory / f"{self.name}-{year}.zip"
        data_paths_in_archive = set()
        # report
        self.logger.info(f"Downloading report {report[0]} from {report[1]}")
        download_path = self.download_directory / report[0]
        await self.download_file(report[1], download_path)
        self.add_to_archive(
            zip_path=zip_path,
            filename=report[0],
            blob=download_path.open("rb"),
        )
        data_paths_in_archive.add(report[0])
        # Don't want to leave multiple giant files on disk, so delete
        # immediately after they're safely stored in the ZIP
        download_path.unlink()
        
        for file_id,filename in file_ids:
            self.logger.info(f"Downloading file {file_id} {uuid}")
#             file_resp = await retry_async(
#                 self.session.post,
#                 ["https://scenarioviewer.nrel.gov/api/download/"],
#                 kwargs={
#                     "data":{"project_uuid": project_uuid, "file_ids": file_record["id"]},
#                     "kwargs":{"allow_redirects":False}},
#             )
            download_path = self.download_directory / filename
            await retry_async(
                _download_file_post, 
                [self.session, "https://scenarioviewer.nrel.gov/api/download/", download_path],
                kwargs={"data":{"project_uuid": uuid, "file_ids": file_id}}
            )
#             await self.download_file(link, download_path)
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

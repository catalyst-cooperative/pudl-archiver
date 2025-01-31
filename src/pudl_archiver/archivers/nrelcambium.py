"""Download NREL Cambium Scenarios data."""

import io
import re
from contextlib import nullcontext
from pathlib import Path

import aiohttp

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.utils import retry_async


async def _download_file_post(
    session: aiohttp.ClientSession, url: str, file: Path | io.BytesIO, **kwargs
):
    async with session.post(url, **kwargs) as response:
        with file.open("wb") if isinstance(file, Path) else nullcontext(file) as f:
            async for chunk in response.content.iter_chunked(1024):
                f.write(chunk)


class NrelCambiumArchiver(AbstractDatasetArchiver):
    """NREL Cambium archiver."""

    name = "nrelcambium"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download NREL Cambium resources."""

        async def post_to_json(url, **kwargs):
            resp = await retry_async(self.session.post, [url], kwargs={"data": kwargs})
            return await retry_async(resp.json)

        project_year_pattern = re.compile(r"Cambium (?P<year>\d{4})")
        report_url_pattern = re.compile(
            r"https://www.nrel.gov/docs/(?P<fy>fy\d{2}osti)/(?P<number>\d{5}\.pdf)"
        )

        project_records = await self.get_json(
            "https://scenarioviewer.nrel.gov/api/projects/"
        )
        for scenario_project in (
            p for p in project_records if p["name"].startswith("Cambium")
        ):
            project_uuid = scenario_project["uuid"]
            m = project_year_pattern.search(scenario_project["name"])
            if not m:
                continue
            project_year = int(m.group("year"))

            report_link_section = "long_description"
            report_links = self.get_hyperlinks_from_text(
                scenario_project[report_link_section], report_url_pattern
            )
            if not report_links:
                raise AssertionError(
                    f"We expect all years to have a {report_link_section} with a link to the report, but {project_year} does not:"
                    f"{scenario_project}"
                )
            report_data = []
            for report_link in report_links:
                m = report_url_pattern.search(report_link)
                if not m:
                    raise AssertionError(
                        f"Bad link {report_link} found in {project_uuid}: {scenario_project}"
                    )
                report_data.append(
                    (f"{m.group('fy')}_{m.group('number')}", report_link)
                )

            file_list = await post_to_json(
                "https://scenarioviewer.nrel.gov/api/file-list/",
                project_uuid=project_uuid,
            )
            yield self.get_year_resource(
                reports=report_data,
                uuid=project_uuid,
                file_ids=[
                    (
                        f["id"],
                        f"NRELCAMBIUM {project_year}  {f['scenario']}  {f['metric']}  {f['time_resolution']}  {f['location_type']}.{f['file_type']}".replace(
                            " ", "_"
                        )
                        .replace("%", "pct")
                        .replace(",", "")
                        .lower(),
                    )
                    for f in file_list["files"]
                    if (f["file_type"] == "CSV" or project_year == 2020)
                ],
                year=project_year,
            )

    async def get_year_resource(
        self, reports, uuid, file_ids, year: int
    ) -> ResourceInfo:
        """Download all available data for a year.

        Resulting resource contains one pdf of the scenario report, and a set of CSVs for different scenarios and geo levels.

        Args:
            links: filename->URL mapping for files to download
            year: the year we're downloading data for
        """
        zip_path = self.download_directory / f"{self.name}-{year}.zip"
        data_paths_in_archive = set()
        # reports
        for report in reports:
            self.logger.info(f"Downloading report {year} {report[0]} from {report[1]}")
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

        for file_id, filename in file_ids:
            self.logger.info(f"Downloading file {year} {file_id} {uuid}")
            download_path = self.download_directory / filename
            await retry_async(
                _download_file_post,
                [
                    self.session,
                    "https://scenarioviewer.nrel.gov/api/download/",
                    download_path,
                ],
                kwargs={"data": {"project_uuid": uuid, "file_ids": file_id}},
            )
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
            # layout=ZipLayout(file_paths=data_paths_in_archive), # can't use ZipLayout bc these CSVs have a multi-row header and pandas throws a tantrum
        )

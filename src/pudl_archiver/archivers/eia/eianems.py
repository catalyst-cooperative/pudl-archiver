"""Download EIA NEMS Github respository."""

import os
import subprocess
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

RELEASE_TO_YEAR_MAP = {
    "AEO2026-Public-Release": 2026,
    "AEO2025-Public-Release": 2025,
    "Initial-GitHub-Release": 2023,
}
"""We manually map the release to the corresponding year of AEO, since there isn't
a consistent way to infer this."""


class EiaNEMSArchiver(AbstractDatasetArchiver):
    """EIA NEMS archiver."""

    name = "eianems"
    # We are archiving multiple versioned releases from the same Git repository, so we can only archive one at a time
    # to avoid trying to access different release versions of the same file at the same time.
    concurrency_limit = 1

    # NEMS files include multiple CSVs with multi-line headers and "placeholder" XML files that
    # trip up our file validations, so we make this a non-blocking test failure.
    fail_on_empty_invalid_files = False

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA NEMS resources."""
        release_json = await self.get_json(
            "https://api.github.com/repos/EIAgov/NEMS/releases"
        )

        release_tags = [release["tag_name"] for release in release_json]
        # Error the archiver if there's a new release
        # so we can update the archiver manually.
        if any(tag not in RELEASE_TO_YEAR_MAP for tag in release_tags):
            raise ValueError(
                f"Unexpected release! Releases: {release_tags}. Investigate and update release_to_year_map to archive."
            )

        # Clone the entire project
        os.chdir(self.download_directory)
        subprocess.run(  # noqa:S603
            ["/usr/bin/git", "clone", "https://github.com/EIAgov/NEMS.git"],
            shell=False,
        )
        os.chdir(self.download_directory / "NEMS")
        subprocess.run(["/usr/bin/git", "lfs", "fetch", "--all"], shell=False)

        for tag in release_tags:
            yield self.get_release_resource(tag=tag)

    async def get_release_resource(self, tag: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a model that produced data for a year of
        AEO data. For example, the initial Github release produced data for the 2023 AEO,
        and has a partition of {year = 2023}.
        """
        year = RELEASE_TO_YEAR_MAP[tag]  # Get AEO year from release name mapping
        zip_path = self.download_directory / f"eianems-{year}.zip"
        data_paths_in_archive = set()

        subprocess.run(["/usr/bin/git", "checkout", tag], shell=False)  # noqa:S603
        # We sanitize tag above using the assertion, so this should be ok.
        subprocess.run(["/usr/bin/git", "lfs", "pull"], shell=False)

        directory = (self.download_directory / "NEMS").resolve()

        for entry in directory.rglob("*"):
            if entry.is_file():
                self.add_to_archive(
                    zip_path=zip_path,
                    filename=str(entry.relative_to(directory)),
                    blob=entry.open("rb"),
                )
                data_paths_in_archive.add(entry.relative_to(directory))

        return ResourceInfo(
            local_path=zip_path,
            partitions={"year": year},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

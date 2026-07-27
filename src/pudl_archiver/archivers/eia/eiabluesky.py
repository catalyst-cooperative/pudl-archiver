"""Download EIA Project BlueSky Github respository."""

import os
import subprocess
from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://github.com/EIAgov/BlueSky/archive/refs/tags"
EXPECTED_TAGS = ["v1.0", "v1.1"]  # Sanitize tags for subprocess calls


class EIABlueSkyArchiver(AbstractDatasetArchiver):
    """EIA Project Blue Sky repository archiver."""

    name = "eiabluesky"
    # We are archiving multiple versioned releases from the same Git repository, so we can only archive one at a time
    # to avoid trying to access different release versions of the same file at the same time.
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA NEMS resources."""
        release_json = await self.get_json(
            "https://api.github.com/repos/EIAgov/BlueSky/releases"
        )

        release_tags = [release["tag_name"] for release in release_json]
        # Error the archiver if there's a new release
        # so we can update the archiver manually.
        if any(tag not in EXPECTED_TAGS for tag in release_tags):
            raise ValueError(
                f"Unexpected release! Releases: {release_tags}. Investigate and update release_to_year_map to archive."
            )

        # Clone the entire project
        os.chdir(self.download_directory)
        subprocess.run(  # noqa:S603
            [
                "/usr/bin/git",
                "clone",
                "https://api.github.com/repos/EIAgov/BlueSky.git",
            ],
            shell=False,
        )
        os.chdir(self.download_directory / "BlueSky")
        subprocess.run(["/usr/bin/git", "lfs", "fetch", "--all"], shell=False)

        for tag in release_tags:
            yield self.get_release_resource(tag=tag)

    async def get_release_resource(self, tag: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a tagged release in the BlueSky
        repository.
        """
        tag_file_name = tag.lower().replace(".", "-")
        zip_path = self.download_directory / f"eiabluesky-{tag_file_name}.zip"
        data_paths_in_archive = set()

        subprocess.run(["/usr/bin/git", "checkout", tag], shell=False)  # noqa:S603
        # We sanitize tag above using the assertion, so this should be ok.
        subprocess.run(["/usr/bin/git", "lfs", "pull"], shell=False)

        directory = (self.download_directory / "BlueSky").resolve()

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
            partitions={"release": tag},
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

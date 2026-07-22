"""Download EIA Project BlueSky Github respository."""

from pathlib import Path

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://github.com/EIAgov/BlueSky/archive/refs/tags"


class EIABlueSkyArchiver(AbstractDatasetArchiver):
    """EIA Project Blue Sky repository archiver."""

    name = "eiabluesky"
    # We are archiving multiple versioned releases from the same Git repository, so we can only archive one at a time
    # to avoid trying to access different release versions of the same file at the same time.
    concurrency_limit = 1

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA Blue Sky resources."""
        release_json = await self.get_json(
            "https://api.github.com/repos/EIAgov/BlueSky/releases"
        )
        release_tags = [release["tag_name"] for release in release_json]
        for tag in release_tags:
            yield self.get_release_resource(tag)

    async def get_release_resource(self, tag: str) -> tuple[Path, dict]:
        """Download entire repo as a zipfile from github from a tagged release.

        A release is expected to correspond to a release version of the code repository.
        """
        tag_file_name = tag.lower().replace(".", "-")
        url = f"{BASE_URL}/{tag}.zip"
        download_path = self.download_directory / f"eiabluesky-{tag_file_name}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"release": tag})

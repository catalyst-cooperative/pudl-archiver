"""Download DOE IRA Energy Community data.

Note that because there are only four links, a registration wall and a tangle of
information on the webpage, we directly connect links to partitions in the metadata. We
don't expect this data to get updated, so this shouldn't pose a problem.
"""

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class DOEIRAECArchiver(AbstractDatasetArchiver):
    """DOE IRA Energy Community archiver."""

    name = "doeiraec"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download DOE IRA Energy Community resources."""
        link_dict = {
            "https://edx.netl.doe.gov/resource/13454403-ef6b-479b-b720-d5e3eaefbb91/download": [
                "msa-nonmsa-fossil-fuel-employment-status",
                2024,
            ],
            "https://edx.netl.doe.gov/resource/4006c9da-f99c-4731-97b2-633cc1578994/download": [
                "coal-closures",
                2024,
            ],
            "https://edx.netl.doe.gov/resource/b736a14f-12a7-4b9f-8f6d-236aa3a84867/download": [
                "msa-nonmsa-fossil-fuel-employment-status",
                2023,
            ],
            "https://edx.netl.doe.gov/resource/28a8eb09-619e-49e5-8ae3-6ddd3969e845/download": [
                "coal-closures",
                2023,
            ],
        }

        for link, partitions in link_dict.items():
            yield self.get_zip_resource(link=link, partitions=partitions)

    async def get_zip_resource(
        self, link: str, partitions: list[str | int]
    ) -> ResourceInfo:
        """Wrapper for downloaded zip file."""
        file_name = "doeiraec-" + "-".join(map(str, partitions)) + ".zip"
        download_path = self.download_directory / file_name
        await self.download_zipfile(link, download_path)
        return ResourceInfo(
            local_path=download_path,
            partitions={"layer": partitions[0], "year": partitions[1]},
        )

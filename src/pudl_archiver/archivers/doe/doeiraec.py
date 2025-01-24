"""Download DoE IRA Community Energy data."""

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

BASE_URL = "https://edx.netl.doe.gov/dataset/ira-energy-community-data-layers"


class DOEIRAECArchiver(AbstractDatasetArchiver):
    """DOE IRA EC archiver."""

    name = "doeiraec"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download DOE IRA EC resources."""
        download_path = self.download_directory / "MSA_NMSA_EC_FFE_v2024_1.zip"
        await self.download_zipfile(
            "https://edx.netl.doe.gov/dataset/ira-energy-community-data-layers/resource/13454403-ef6b-479b-b720-d5e3eaefbb91/download",
            download_path,
        )
        yield self.get_zip_resource(download_path, 2024)

        download_path = self.download_directory / "Coal_Closures_EnergyComm_v2024_1.zip"
        await self.download_zipfile(
            "https://edx.netl.doe.gov/dataset/ira-energy-community-data-layers/resource/4006c9da-f99c-4731-97b2-633cc1578994/download",
            download_path,
        )
        yield self.get_zip_resource(download_path, 2024)

    async def get_zip_resource(self, link: str, year: int) -> ResourceInfo:
        """Wrapper for downloaded zip file."""
        return ResourceInfo(local_path=link, partitions={"year": year})

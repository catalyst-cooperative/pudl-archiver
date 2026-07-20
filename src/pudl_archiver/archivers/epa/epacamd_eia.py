"""Download EPA CAMD data."""

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)


class EpaCamdEiaArchiver(AbstractDatasetArchiver):
    """EPA CAMD archiver."""

    name = "epacamd_eia"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download entire repo as a zipfile from github.

        The EPA developed the original version of the crosswalk, but this has been dormant
        for several years and only uses EIA data from 2018. We have a fork of this repo,
        which we've modified slightly to run with later years of data. For now, the
        simplest solution is to use the 2018 data from the EPA repo and the latest data
        from our fork as static outputs. At some point it would be best to either
        integrate the notebook into our ETL so we can dynamically run it with all years
        of interest, or develop our own linkage.
        """
        yield self.get_2018()
        yield self.get_latest_years()

    async def get_latest_years(self) -> ResourceInfo:
        """Get latest version from our forked repo."""
        resources = []
        for year in [2019, 2020, 2021, 2022, 2023, 2024]:
            url = f"https://github.com/catalyst-cooperative/camd-eia-crosswalk-latest/archive/refs/tags/v{year}.zip"
            download_path = self.download_directory / f"epacamd_eia_{year}.zip"
            await self.download_zipfile(url, download_path)

            resources.append(
                ResourceInfo(local_path=download_path, partitions={"year": year})
            )
        return resources

    async def get_2018(self) -> ResourceInfo:
        """Get 2018 data from EPA repo."""
        url = (
            "https://github.com/USEPA/camd-eia-crosswalk/archive/refs/heads/master.zip"
        )
        download_path = self.download_directory / "epacamd_eia_2018.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": 2018})

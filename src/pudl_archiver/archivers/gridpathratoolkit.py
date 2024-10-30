"""Archive GridPath RA toolkit renewable generation data.

This dataset was produced by Moment Energy Insights (now Sylvan Energy Analytics).
It is archived from files stored in the private sources.catalyst.coop bucket.
"""

import logging
import zipfile
from pathlib import Path

from google.cloud import storage

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout
from pudl_archiver.utils import add_to_archive_stable_hash

logger = logging.getLogger(f"catalystcoop.{__name__}")


class GridPathRAToolkitArchiver(AbstractDatasetArchiver):
    """GridPath RA Toolkit renewable generation profiles archiver."""

    name = "gridpathratoolkit"
    bucket_name = "sources.catalyst.coop"

    rename_dict = {
        "TemporalData/HourlySolar_byProject.zip": "original_solar_capacity.zip",
        "TemporalData/HourlyWind_byProject.zip": "original_wind_capacity.zip",
        "MonteCarlo_Inputs/temporal_data/wind/": "aggregated_wind_capacity.zip",
        "MonteCarlo_Inputs/temporal_data/wind_syn/": "aggregated_extended_wind_capacity.zip",
        "MonteCarlo_Inputs/temporal_data/solar/": "aggregated_solar_capacity.zip",
        "MonteCarlo_Inputs/temporal_data/solar_syn/": "aggregated_extended_solar_capacity.zip",
        "TemporalData/DailyWeatherData_cleaned.csv": "daily_weather.csv",
        "SolarAggregations.csv": "solar_capacity_aggregations.csv",
        "WindAggregations.csv": "wind_capacity_aggregations.csv",
        "GridPath_RA_Toolkit_HowTo.pdf": "gridpathratoolkit_howto.pdf",
        "GridPath_RA_Toolkit_Report_2022-10-12.pdf": "gridpathratoolkit_report_2022_10_12.pdf",
        "TemporalData/readme.txt": "readme.txt",
    }

    async def get_resources(self) -> ArchiveAwaitable:
        """Download GridPath RA Toolkit resources."""
        bucket = storage.Client().get_bucket(self.bucket_name)

        for original_file in self.rename_dict:
            if "MonteCarlo_Inputs" not in original_file:
                yield self.get_gcs_resource(original_file, bucket)
            # Handle resources that need to be zipped separately
            else:
                yield self.get_and_zip_resources(original_file, bucket)

    async def get_gcs_resource(
        self, original_file: str, bucket: storage.Bucket
    ) -> tuple[Path, dict]:
        """Download GridPath RA Toolkit data files from GCS.

        There are several types of files: a documentation PDF, a series of zipped files,
        a series of CSV files,  and a series of files to download from within a
        particular folder and zip before archiving.
        """
        file_name = self.rename_dict[original_file]
        path_to_file = self.download_directory / file_name
        blobs = bucket.list_blobs(
            prefix=f"{self.name}/{original_file}"
        )  # Get all blobs in folder

        for i, blob in enumerate(blobs):
            if i > 1:
                raise AssertionError(
                    f"More than one matching file found for {file_name}: {blob}"
                )

            # Download blob to local file
            # We download the entire zipfile to avoid having to authenticate using
            # a second GCS library, since GCS doesn't support fsspec file paths.
            logger.info(f"Downloading {blob.name} to {path_to_file}")

            blob.download_to_filename(path_to_file)

            # The partition should be the filename without the filetype extension.
            # E.g., solar_capacity_aggregations.csv has part: solar_capacity_aggregations

        part = file_name.split(".")[0]  # Remove .csv/.zip extension
        return ResourceInfo(
            local_path=path_to_file,
            partitions={"part": part},
        )

    async def get_and_zip_resources(
        self, original_file: str, bucket: storage.Bucket
    ) -> tuple[Path, dict]:
        """Download folder from GCS and zip the files, then upload to Zenodo."""
        blobs = bucket.list_blobs(
            prefix=f"{self.name}/{original_file}"
        )  # Get all blobs in folder

        # Get name and path of final file
        final_zipfile_name = self.rename_dict[original_file]
        archive_path = self.download_directory / final_zipfile_name

        data_paths_in_archive = set()

        with zipfile.ZipFile(
            archive_path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            for blob in blobs:
                if blob.name.endswith("/"):
                    continue

                # Download all files locally
                logger.info(f"Downloading {blob.name} to {final_zipfile_name}")
                string = blob.download_as_string()
                add_to_archive_stable_hash(
                    archive=archive, filename=blob.name.split("/")[-1], data=string
                )
                data_paths_in_archive.add(blob.name.split("/")[-1])

        # The partition should be the filename without the filetype extension.
        # E.g., solar_capacity_aggregations.csv has part: solar_capacity_aggregations

        return ResourceInfo(
            local_path=archive_path,
            partitions={
                "part": final_zipfile_name.split(".")[0]
            },  # Drop file type suffix
            layout=ZipLayout(file_paths=data_paths_in_archive),
        )

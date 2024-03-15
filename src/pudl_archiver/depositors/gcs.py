"""Handle all deposition actions using a GCS bucket as the backend.

Note: this is an experimental feature developed specifically for ongoing development
of the SEC-EIA record linkage project. Normal production archives should continue
using the zenodo depositor and relying on the Datastore to cache archives on GCS.
"""

import logging
import traceback
import zipfile
from pathlib import Path
from typing import BinaryIO

import aiohttp
import pandas as pd
import pg8000
from google.cloud import storage
from google.cloud.sql.connector import Connector
from pydantic import Field, PrivateAttr
from sqlalchemy import Engine, create_engine

from pudl_archiver.depositors.depositor import (
    AbstractDepositorInterface,
    DepositionAction,
    DepositionChange,
)
from pudl_archiver.frictionless import MEDIA_TYPES, DataPackage, Resource, ResourceInfo
from pudl_archiver.utils import RunSettings, Url, compute_md5

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _resource_from_blob(
    name: str, blob: storage.Blob, parts: dict[str, str]
) -> Resource:
    """Create a resource from a blob with partitions.

    Args:
        blob: GCS blob.
        parts: Working partitions of current resource.
    """
    file_path = Path(name)
    mt = MEDIA_TYPES[file_path.suffix[1:]]
    return Resource(
        name=name,
        path=blob.path,
        remote_url=blob.path,
        title=file_path.name,
        mediatype=mt,
        parts=parts,
        bytes=blob.size,
        hash=blob.md5_hash,
        format=file_path.suffix,
    )


class GCSDepositor(AbstractDepositorInterface):
    """Implements AbstractDepositorInterface interface for GCS backend."""

    dataset: str
    bucket_name: str = Field(validation_alias="GCS_BUCKET_NAME")
    metadata_db_instance_connection: str = Field(
        validation_alias="GCS_METADATA_DB_INSTANCE_CONNECTION"
    )
    user: str = Field(validation_alias="GCS_IAM_USER")
    metadata_db_name: str = Field(validation_alias="GCS_METADATA_DB_NAME")
    _bucket = PrivateAttr()
    _engine = PrivateAttr()

    @classmethod
    async def get_latest_version(
        cls,
        dataset: str,
        session: aiohttp.ClientSession,
        run_settings: RunSettings,
    ) -> "GCSDepositor":
        """Create a new ZenodoDepositor.

        Args:
            dataset: Name of dataset to archive.
            session: HTTP handler - we don't use it directly, it's wrapped in self.request.
            run_settings: Settings from CLI.
        """
        if dataset != "sec10k":
            raise RuntimeError("GCS depositor is only implemented for sec10k.")

        self = cls(dataset=dataset)
        storage_client = storage.Client()
        self._bucket = storage_client.bucket(self.bucket_name)
        self._engine = self._get_engine()
        return self

    def _get_engine(self) -> Engine:
        """Initialize a connection pool for a Cloud SQL instance of Postgres.

        Uses the Cloud SQL Python Connector with Automatic IAM Database Authentication.
        """
        # initialize Cloud SQL Python Connector object
        connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                self.metadata_db_instance_connection,
                "pg8000",
                user=self.user,
                db=self.metadata_db_name,
                enable_iam_auth=True,
            )
            return conn

        return create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )

    def _get_blob(self, filename: str, base_path: str | None = None) -> storage.Blob:
        """Return a GCS blob pointing to filename."""
        if base_path:
            bucket = self._bucket.blob(f"{self.dataset}/{base_path}/{filename}")
        else:
            bucket = self._bucket.blob(f"{self.dataset}/{filename}")
        return bucket

    async def open_draft(self) -> "AbstractDepositorInterface":
        """Open a new draft deposition to make edits."""
        return self

    async def publish(self) -> "AbstractDepositorInterface":
        """Publish draft deposition and return new depositor with updated deposition."""
        pass

    async def get_file(self, filename: str) -> bytes | None:
        """Get file from deposition.

        Args:
            filename: Name of file to fetch.
        """
        file_blob = self._get_blob(filename)
        file = file_blob.download_as_bytes() if file_blob.exists() else None
        return file

    async def list_files(self) -> list[str]:
        """Return list of filenames from previous version of deposition."""
        return [
            name.replace(f"{self.dataset}/", "")
            for name in self._bucket.list_blobs(match_glob=f"{self.dataset}/*")
        ]

    def get_deposition_link(self) -> Url:
        """Get URL which points to deposition."""
        return self._bucket.path

    async def create_file(
        self, filename: str, data: BinaryIO, metadata: pd.DataFrame | None = None
    ):
        """Extract zipfiles into subdirectories in bucket.

        Args:
            target: the filename of the file you want to create.
            data: the actual data associated with the file.

        Returns:
            None if success.
        """
        if not filename.endswith(".zip"):
            logger.info(f"Uploading {filename} to {self._bucket}")
            file_blob = self._get_blob(filename)
            file_blob.upload_from_file(data)
            return

        base_path = filename.replace(".zip", "")
        with zipfile.ZipFile(data) as archive:
            for fname in archive.namelist():
                with archive.open(fname) as f:
                    logger.info(f"Uploading {fname} to {self._bucket}")
                    file_blob = self._get_blob(fname, base_path=base_path)
                    file_blob.upload_from_file(f)

        if metadata is not None:
            metadata.to_sql(
                f"{self.dataset}_metadata",
                self._engine,
                method="multi",
                if_exists="append",
                index=False,
            )

    async def delete_file(
        self,
        filename: str,
    ):
        """Delete a file from a deposition.

        Args:
            target: the filename of the file you want to delete.

        Returns:
            None if success.
        """
        pass

    def generate_change(
        self, filename: str, resource: ResourceInfo
    ) -> DepositionChange | None:
        """Check whether file should be changed.

        Args:
            filename: Name of file in question.
            resource: Info about downloaded file.
        """
        file_blob = self._get_blob(filename)
        action = None
        if not file_blob.exists():
            action = DepositionAction.CREATE
        else:
            if (local_md5 := compute_md5(resource.local_path)) != file_blob.md5_hash:
                logger.info(
                    f"Updating {filename}: local hash {local_md5} vs. remote {file_blob.md5_hash}"
                )
                action = DepositionAction.UPDATE

        if action is None:
            return None

        return DepositionChange(
            action_type=action,
            name=filename,
            resource=resource.local_path,
        )

    async def cleanup_after_error(self, e: Exception):
        """Cleanup draft after an error during an archive run."""
        logger.error(
            f"Failed while creating new deposition: {traceback.print_exception(e)}"
        )

    def generate_datapackage(self, resources: dict[str, ResourceInfo]) -> DataPackage:
        """Generate new datapackage and return it."""
        logger.info(f"Creating new datapackage.json for {self.dataset_id}")

        # Create updated datapackage
        resources = [
            _resource_from_blob(name, self._get_blob(name), info.partitions)
            for name, info in resources.items()
            if name != "datapackage.json"
        ]
        datapackage = DataPackage.new_datapackage(
            self.dataset,
            resources,
            self.deposition.metadata.version,
        )

        return datapackage

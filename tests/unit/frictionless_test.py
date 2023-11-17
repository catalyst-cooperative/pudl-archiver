"""Test frictionless structures."""
import random
from pathlib import Path

from frictionless.package import Package
from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.frictionless import DataPackage
from pudl_archiver.zenodo.entities import DepositionFile, FileLinks


def test_datapackage():
    """This test verfies that Datapackage produces a valid frictionless descriptor."""
    files = ["test_file1.zip", "test_file2.csv", "test_file3.xlsx"]

    # Create fake inputs
    deposition_files = [
        DepositionFile(
            checksum="fake_hash",
            filename=name,
            id="fake_id",
            filesize=random.randint(1, 10000),  # noqa: S311
            links=FileLinks(download="https://fake.url.com"),
        )
        for name in files
    ]
    resources = {
        name: ResourceInfo(local_path=Path(f"/fake/directory/{name}"), partitions={})
        for name in files
    }

    dp = DataPackage.from_filelist(
        "ferc1", deposition_files, resources, version="1.0.0"
    )

    assert Package(descriptor=dp.dict(by_alias=True)).metadata_valid

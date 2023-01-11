"""Test frictionless structures."""
import random
from pathlib import Path

from frictionless.package import Package

from pudl_archiver.zenodo.entities import DepositionFile, DepositionLinks
from pudl_archiver.frictionless import Datapackage, Resource
from pudl_archiver.archivers.classes import ResourceInfo


def test_datapackage():
    files = ["test_file1.zip", "test_file2.csv", "test_file3.xlsx"]

    # Create fake inputs
    deposition_files = [
        DepositionFile(
            checksum="fake_hash",
            filename=name,
            id="fake_id",
            filesize=random.randint(1, 10000),
            links=DepositionLinks()
        )
        for name in files
    ]
    resources = {name: ResourceInfo(local_path=Path(f"/fake/directory/{name}"), partitions={}) for name in files}

    dp = Datapackage.from_filelist("ferc1", deposition_files, resources)

    assert Package(descriptor=dp.dict(by_alias=True)).metadata_valid


def test_resource_types():
    files = ["test_file1.zip", "test_file2.csv", "test_file3.xlsx"]

    # Create fake inputs
    for name in files:
        file = DepositionFile(
            checksum="fake_hash",
            filename=name,
            id="fake_id",
            filesize=random.randint(1, 10000),
            links=DepositionLinks()
        )

        assert Resource.from_file(file, {})

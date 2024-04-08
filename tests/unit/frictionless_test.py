"""Test frictionless structures."""

import random
from pathlib import Path

from frictionless.package import Package
from pudl_archiver.archivers.classes import ResourceInfo
from pudl_archiver.depositors.zenodo.depositor import _resource_from_file
from pudl_archiver.depositors.zenodo.entities import DepositionFile, FileLinks
from pudl_archiver.frictionless import DataPackage


def test_zenodo_datapackage():
    """This test verfies that Datapackage produces a valid frictionless descriptor."""
    files = ["test_file1.zip", "test_file2.csv", "test_file3.xlsx"]

    # Create fake inputs
    deposition_files = [
        DepositionFile(
            checksum="fake_hash",
            filename=name,
            id="fake_id",
            filesize=random.randint(1, 10000),  # noqa: S311
            links=FileLinks(
                download="https://fake.zenodo.org/api/records/100/files/bogus"
            ),
        )
        for name in files
    ]
    resource_info = {
        name: ResourceInfo(local_path=Path(f"/fake/directory/{name}"), partitions={})
        for name in files
    }
    resources = [
        _resource_from_file(f, resource_info[f.filename].partitions)
        for f in deposition_files
        if f.filename != "datapackage.json"
    ]

    dp = DataPackage.new_datapackage("ferc1", resources, version="1.0.0")

    assert Package(descriptor=dp.model_dump(by_alias=True)).metadata_valid

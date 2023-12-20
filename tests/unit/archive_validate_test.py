"""Test archive validate module."""
import copy
import itertools
import json
import logging
import zipfile
from pathlib import Path

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta
from pudl_archiver.archivers import validate
from pudl_archiver.frictionless import DataPackage, Resource, ZipLayout
from pudl_archiver.utils import Url

logger = logging.getLogger(f"catalystcoop.{__name__}")



@pytest.mark.parametrize(
    "baseline_partitions,new_partitions,diffs",
    [
        (
            {"part0": "val0", "part1": "val1"},
            {"part0": "val0", "part1": "val1"},
            [],
        ),
        (
            {"part0": "val0", "part1": "val1"},
            {"part0": "val0", "part1": "val1_changed"},
            [
                validate.PartitionDiff(
                    key="part1",
                    value="val1_changed",
                    previous_value="val1",
                    diff_type="UPDATE",
                )
            ],
        ),
        (
            {"part0": "val0", "part1": "val1_deleted"},
            {"part0": "val0"},
            [
                validate.PartitionDiff(
                    key="part1",
                    previous_value="val1_deleted",
                    diff_type="DELETE",
                )
            ],
        ),
        (
            {"part0": "val0"},
            {"part0": "val0", "part1": "val1_created"},
            [
                validate.PartitionDiff(
                    key="part1",
                    value="val1_created",
                    diff_type="CREATE",
                )
            ],
        ),
    ],
)
def test_process_partition_diffs(baseline_partitions, new_partitions, diffs):
    """Test partition diffs."""
    test_diffs = validate._process_partition_diffs(baseline_partitions, new_partitions)
    assert test_diffs == diffs


def _fake_resource(num=0, **kwargs):
    params = {
        "name": f"resource{num}",
        "path": "https://www.fake.link",
        "remote_url": "https://www.fake.link",
        "title": f"Resource {num}",
        "parts": {},
        "mediatype": "zip",
        "format": "format",
        "bytes": 10,
        "hash": f"hash{num}",
    } | kwargs
    return Resource(**params)


@pytest.mark.parametrize(
    "baseline_resources,new_resources,diffs",
    [
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=40, hash="hash1_changed"),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="UPDATE",
                    size_diff=20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="DELETE",
                    size_diff=-20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                validate.FileDiff(
                    name="resource1",
                    diff_type="CREATE",
                    size_diff=20,
                )
            ],
        ),
        (
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [
                _fake_resource(num=0),
                _fake_resource(num=1, bytes=20),
            ],
            [],
        ),
    ],
)
def test_create_run_summary(baseline_resources, new_resources, diffs, mocker):
    """Test resource diffs."""
    baseline = mocker.MagicMock(
        resources=baseline_resources, version="0.0.1", created="2023-01-01"
    )
    new = mocker.MagicMock(
        resources=new_resources, version="0.0.2", created="2023-01-02"
    )
    summary = validate.RunSummary.create_summary(
        name="test package",
        baseline_datapackage=baseline,
        new_datapackage=new,
        validation_tests=[],
        record_url=Url("https://www.catalyst.coop/bogus-record-url"),
    )
    test_diffs = summary.file_changes
    assert test_diffs == diffs


def _zip_factory(base_dir: Path, files: list[Path]) -> Path:
    zip_path = base_dir / "tmp.zip"
    with zipfile.ZipFile(zip_path, "w") as resource:
        for file_path in files:
            with resource.open(str(file_path), "w") as f:
                f.write(b"Test data.")

    return zip_path


@pytest.mark.parametrize(
    "expected_files,extra_files,missing_files,invalid_files",
    [
        (
            [Path("file1.xml"), Path("dir/file2.xlsx"), Path("dir/subdir/file3.json")],
            [],
            [],
            [],
        ),
        (
            [Path("file1.xml"), Path("dir/file2.xlsx"), Path("dir/subdir/file3.json")],
            [Path("file4.xml")],
            [],
            [],
        ),
        (
            [Path("file1.xml"), Path("dir/file2.xlsx"), Path("dir/subdir/file3.json")],
            [],
            [Path("file1.xml")],
            [],
        ),
        (
            [Path("file1.xml"), Path("dir/file2.xlsx"), Path("dir/subdir/file3.json")],
            [],
            [],
            [Path("dir/file2.xlsx")],
        ),
        (
            [Path("file1.xml"), Path("dir/file2.xlsx"), Path("dir/subdir/file3.json")],
            [Path("file4.xml")],
            [Path("file1.xml")],
            [Path("dir/file2.xlsx")],
        ),
    ],
)
def test_zip_layout_validation(
    expected_files, extra_files, missing_files, invalid_files, tmp_path, mocker
):
    """Test validation of zip file layout."""
    zip_files = [
        f
        for f in itertools.chain(expected_files, extra_files)
        if f not in missing_files
    ]
    zip_path = _zip_factory(tmp_path, zip_files)
    mocker.patch(
        "pudl_archiver.archivers.validate._validate_file_type",
        side_effect=lambda path, buffer: path not in invalid_files,
    )

    layout = ZipLayout(file_paths=expected_files)
    success, notes = layout.validate_zip(zip_path)

    if extra_files:
        assert (
            f"{zip_path.name} contains unexpected files: {list(map(str, extra_files))}"
            in notes
        )

    if missing_files:
        assert (
            f"{zip_path.name} is missing files: {list(map(str, missing_files))}"
            in notes
        )

    for invalid_file in invalid_files:
        assert f"The file, {str(invalid_file)}, in {zip_path.name} is invalid." in notes

    assert success == (
        (len(extra_files) == 0)
        and (len(missing_files) == 0)
        and (len(invalid_files) == 0)
    )


@pytest.mark.parametrize(
    "specs,expected_success",
    [
        (
            [
                {"required_for_run_success": True, "success": True},
                {"required_for_run_success": True, "success": True},
            ],
            True,
        ),
        (
            [
                {"required_for_run_success": True, "success": True},
                {"required_for_run_success": True, "success": False},
            ],
            False,
        ),
        (
            [
                {"required_for_run_success": True, "success": True},
                {"required_for_run_success": False, "success": False},
            ],
            True,
        ),
        (
            [
                {"required_for_run_success": True, "success": True},
                {"required_for_run_success": False, "success": True},
            ],
            True,
        ),
        (
            [],
            True,
        ),
    ],
)
def test_run_summary_success(specs, expected_success):
    validations = [
        validate.ValidationTestResult(
            name=f"test{i}",
            description=f"test{i}",
            required_for_run_success=spec["required_for_run_success"],
            success=spec["success"],
        )
        for i, spec in enumerate(specs)
    ]
    summary = validate.RunSummary(
        dataset_name="test",
        validation_tests=validations,
        file_changes=[],
        date="2023-11-29",
        previous_version_date="2023-11-28",
        record_url=Url("https://www.catalyst.coop/bogus-record-url"),
    )
    assert summary.success == expected_success


# Test inputs for test_validate_data_continuity function
fake_new_datapackage_quarter_success = DataPackage.model_validate_json(
    json.dumps(
        {
            "name": "epacems",
            "title": "Test EPACEMS",
            "description": "Describe EPACEMS",
            "keywords": ["epa"],
            "contributors": [
                {
                    "title": "Catalyst Cooperative",
                    "path": "https://catalyst.coop/",
                    "email": "pudl@catalyst.coop",
                    "role": "publisher",
                    "zenodo_role": "distributor",
                    "organization": "Catalyst Cooperative",
                    # "orcid": null
                }
            ],
            "sources": [{"blah": "blah"}],
            "profile": "data-package",
            "homepage": "https://catalyst.coop/pudl/",
            "licenses": [
                {
                    "name": "other-pd",
                    "title": "U.S. Government Works",
                    "path": "https://www.usa.gov/publicdomain/label/1.0/",
                }
            ],
            "resources": [
                {
                    "profile": "data-resource",
                    "name": "epacems-1995.zip",
                    "path": "https://zenodo.org/records/10306114/files/epacems-1995.zip",
                    "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995.zip",
                    "title": "epacems-1995.zip",
                    "parts": {
                        "year_quarter": [
                            "1995q1",
                            "1995q2",
                            "1995q3",
                            "1995q4",
                        ]
                    },
                    "encoding": "utf-8",
                    "mediatype": "application/zip",
                    "format": ".zip",
                    "bytes": 18392692,
                    "hash": "f4ab7fbaa673fa7592feb6977cc41611",
                },
                {
                    "profile": "data-resource",
                    "name": "epacems-1996.zip",
                    "path": "https://zenodo.org/records/10306114/files/epacems-1996.zip",
                    "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996.zip",
                    "title": "epacems-1996.zip",
                    "parts": {
                        "year_quarter": [
                            "1996q1",
                            "1996q2",
                            "1996q3",
                            "1996q4",
                        ]
                    },
                    "encoding": "utf-8",
                    "mediatype": "application/zip",
                    "format": ".zip",
                    "bytes": 18921381,
                    "hash": "fb35601de4e849a6cd123e32f59623f9",
                },
                {
                    "profile": "data-resource",
                    "name": "epacems-1997.zip",
                    "path": "https://zenodo.org/records/10306114/files/epacems-1997.zip",
                    "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997.zip",
                    "title": "epacems-1997.zip",
                    "parts": {
                        "year_quarter": [
                            "1997q1",
                            "1997q2",
                        ]
                    },
                    "encoding": "utf-8",
                    "mediatype": "application/zip",
                    "format": ".zip",
                    "bytes": 21734463,
                    "hash": "e9ece6bb8190e14d44834bf0a068ac5d",
                },
            ],
            "created": "2023-11-30 20:51:43.255388",
            "version": "1",
        }
    )
)
fake_new_datapackage_month_success = copy.deepcopy(fake_new_datapackage_quarter_success)
# Make a successful monthly datapackage based on the quarterly one
for resource in fake_new_datapackage_month_success.resources:
    resource_months_min = pd.to_datetime(min(resource.parts["year_quarter"]))
    resource_months_max = pd.to_datetime(
        max(resource.parts["year_quarter"])
    ) + relativedelta(months=3)
    dd_idx = pd.date_range(start=resource_months_min, end=resource_months_max, freq="M")
    resource.parts = {
        "year_month": [pd.to_datetime(x).strftime("%Y-%m") for x in dd_idx]
    }
# Fails because it's missing 1997q1 - (last year, non-consecutive)
fake_new_datapackage_quarter_fail1 = copy.deepcopy(fake_new_datapackage_quarter_success)
fake_new_datapackage_quarter_fail1.resources[2].parts["year_quarter"] = ["1997q2"]
# Fails because it's missing 1997-02 - (last year, non-consecutive)
fake_new_datapackage_month_fail1 = copy.deepcopy(fake_new_datapackage_month_success)
fake_new_datapackage_month_fail1.resources[2].parts["year_month"] = [
    "1997-01",
    "1997-03",
]
# Fails because it's missing 1996q4 - (middle year, non-complete)
fake_new_datapackage_quarter_fail2 = copy.deepcopy(fake_new_datapackage_quarter_success)
fake_new_datapackage_quarter_fail2.resources[1].parts["year_quarter"] = [
    "1996q1",
    "1996q2",
    "1996q3",
]
# Fails because it's missing the rest of the months after 03.
fake_new_datapackage_month_fail2 = copy.deepcopy(fake_new_datapackage_month_success)
fake_new_datapackage_month_fail2.resources[1].parts["year_month"] = ["1996-01", "1996-02", "1996-03"]
# Test one year of data
fake_new_datapackage_month_fail3 = copy.deepcopy(fake_new_datapackage_month_success)
fake_new_datapackage_month_fail3.resources = [fake_new_datapackage_month_fail3.resources[0]]


@pytest.mark.parametrize(
    "new_datapackage,success",
    [
        (fake_new_datapackage_quarter_success, True),
        (fake_new_datapackage_month_success, True),
        (fake_new_datapackage_quarter_fail1, False),
        (fake_new_datapackage_month_fail1, False),
        (fake_new_datapackage_quarter_fail2, False),
        (fake_new_datapackage_month_fail2, False),
        (fake_new_datapackage_month_fail3, True),
    ],
)
def test_validate_data_continuity(new_datapackage, success):
    """Test the dataset archiving valiation for epacems."""
    validation = validate.validate_data_continuity(new_datapackage)
    logger.info(validation)
    if validation["success"] != success:
        raise AssertionError(
            f"Expected test success to be {success} but it was {validation['success']}."
        )

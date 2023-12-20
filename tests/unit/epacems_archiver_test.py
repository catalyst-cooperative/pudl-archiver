import copy
import json

import pytest
from pudl_archiver.archivers.epacems import EpaCemsArchiver
from pudl_archiver.frictionless import DataPackage, ResourceInfo

fake_new_datapackage_success = DataPackage.model_validate_json(
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
                    "parts": {"year_quarter": [
                        "1995q1",
                        "1995q2",
                        "1995q3",
                        "1995q4",
                    ]},
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
                    "parts": {"year_quarter": [
                        "1996q1",
                        "1996q2",
                        "1996q3",
                        "1996q4",
                    ]},
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
                    "parts": {"year_quarter": [
                        "1997q1",
                        "1997q2",
                    ]},
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
# Make a fake baseline datapackage to pass to the validation function (not used)
fake_baseline_datapackage = copy.deepcopy(fake_new_datapackage_success)
fake_baseline_datapackage.resources = [fake_baseline_datapackage.resources[0]]
# Fails because it's missing 1997q1 - (last year, non-consecutive)
fake_new_datapackage_fail1 = copy.deepcopy(fake_new_datapackage_success)
fake_new_datapackage_fail1.resources[2].parts["year_quarter"] = ["1997q2"]
# Fails because it's missing 1996q4 - (middle year, non-complete)
fake_new_datapackage_fail2 = copy.deepcopy(fake_new_datapackage_success)
fake_new_datapackage_fail2.resources[1].parts["year_quarter"] = ["1996q1", "1996q2", "1996q3"]

fake_resource = {"epacems:": ResourceInfo(local_path="/blah/blah", partitions={})}


@pytest.mark.parametrize(
    "baseline_datapackage,new_datapackage,resources,success",
    [
        (fake_baseline_datapackage, fake_new_datapackage_success, fake_resource, True),
        (fake_baseline_datapackage, fake_new_datapackage_fail1, fake_resource, False),
        (fake_baseline_datapackage, fake_new_datapackage_fail2, fake_resource, False),
    ],
)
def test_dataset_validate_archive(
    baseline_datapackage, new_datapackage, resources, success
):
    """Test the dataset archiving valiation for epacems."""
    archiver = EpaCemsArchiver("mock_session")
    validation = archiver.dataset_validate_archive(
        baseline_datapackage, new_datapackage, resources
    )
    if validation["success"] != success:
        raise AssertionError(
            f"Expected test success to be {success} but it was {validation['success']}."
        )

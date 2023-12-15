import json

import pytest
from pudl_archiver.archivers.epacems import EpaCemsArchiver
from pudl_archiver.frictionless import DataPackage, ResourceInfo

fake_baseline_datapackage = DataPackage.model_validate_json(json.dumps({
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
            #"orcid": null
        }
    ],
    "sources": [{"blah": "blah"}],
    "profile": "data-package",
    "homepage": "https://catalyst.coop/pudl/",
    "licenses": [
        {
            "name": "other-pd",
            "title": "U.S. Government Works",
            "path": "https://www.usa.gov/publicdomain/label/1.0/"
        }
    ],
    "resources": [
        {
            "profile": "data-resource",
            "name": "epacems-1995-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "title": "epacems-1995-1.zip",
            "parts": {
                "year_quarter": "1995q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18392692,
            "hash": "f4ab7fbaa673fa7592feb6977cc41611"
        },
    ],
    "created": "2023-11-30 20:51:43.255388",
    "version": "0",
}))
fake_new_datapackage_success = DataPackage.model_validate_json(json.dumps({
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
            #"orcid": null
        }
    ],
    "sources": [{"blah": "blah"}],
    "profile": "data-package",
    "homepage": "https://catalyst.coop/pudl/",
    "licenses": [
        {
            "name": "other-pd",
            "title": "U.S. Government Works",
            "path": "https://www.usa.gov/publicdomain/label/1.0/"
        }
    ],
    "resources": [
        {
            "profile": "data-resource",
            "name": "epacems-1995-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "title": "epacems-1995-1.zip",
            "parts": {
                "year_quarter": "1995q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18392692,
            "hash": "f4ab7fbaa673fa7592feb6977cc41611"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "title": "epacems-1995-2.zip",
            "parts": {
                "year_quarter": "1995q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18921381,
            "hash": "fb35601de4e849a6cd123e32f59623f9"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "title": "epacems-1995-3.zip",
            "parts": {
                "year_quarter": "1995q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 21734463,
            "hash": "e9ece6bb8190e14d44834bf0a068ac5d"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-4.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "title": "epacems-1995-4.zip",
            "parts": {
                "year_quarter": "1995q4"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 19070179,
            "hash": "c3ca1f2095564d4d7089c5d3d26e5fed"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "title": "epacems-1996-1.zip",
            "parts": {
                "year_quarter": "1996q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18886451,
            "hash": "d6e255f46a597b3087019752885bb9b7"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "title": "epacems-1996-2.zip",
            "parts": {
                "year_quarter": "1996q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18397735,
            "hash": "129c4a95d4ea6c3fa1b9f173499a95e4"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "title": "epacems-1996-3.zip",
            "parts": {
                "year_quarter": "1996q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 20276462,
            "hash": "2c23b7f09dc8c92a7a45e80502aef588"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-4.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-4.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-4.zip",
            "title": "epacems-1996-4.zip",
            "parts": {
                "year_quarter": "1996q4"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18399321,
            "hash": "52fb801ac36591a3a4a9cd0d60d1842b"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1997-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1997-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997-1.zip",
            "title": "epacems-1997-1.zip",
            "parts": {
                "year_quarter": "1997q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 67290767,
            "hash": "fd263d6ce405419aa5d6e4d14c452739"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1997-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "title": "epacems-1997-2.zip",
            "parts": {
                "year_quarter": "1997q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 68831002,
            "hash": "9ea4ff4d3176f4f5c7fadd2abf649fa8"
        },
    ],
    "created": "2023-11-30 20:51:43.255388",
    "version": "1",
}))
fake_new_datapackage_fail1 = DataPackage.model_validate_json(json.dumps({
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
            #"orcid": null
        }
    ],
    "sources": [{"blah": "blah"}],
    "profile": "data-package",
    "homepage": "https://catalyst.coop/pudl/",
    "licenses": [
        {
            "name": "other-pd",
            "title": "U.S. Government Works",
            "path": "https://www.usa.gov/publicdomain/label/1.0/"
        }
    ],
    "resources": [
        {
            "profile": "data-resource",
            "name": "epacems-1995-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "title": "epacems-1995-1.zip",
            "parts": {
                "year_quarter": "1995q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18392692,
            "hash": "f4ab7fbaa673fa7592feb6977cc41611"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "title": "epacems-1995-2.zip",
            "parts": {
                "year_quarter": "1995q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18921381,
            "hash": "fb35601de4e849a6cd123e32f59623f9"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "title": "epacems-1995-3.zip",
            "parts": {
                "year_quarter": "1995q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 21734463,
            "hash": "e9ece6bb8190e14d44834bf0a068ac5d"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-4.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "title": "epacems-1995-4.zip",
            "parts": {
                "year_quarter": "1995q4"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 19070179,
            "hash": "c3ca1f2095564d4d7089c5d3d26e5fed"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "title": "epacems-1996-1.zip",
            "parts": {
                "year_quarter": "1996q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18886451,
            "hash": "d6e255f46a597b3087019752885bb9b7"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "title": "epacems-1996-2.zip",
            "parts": {
                "year_quarter": "1996q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18397735,
            "hash": "129c4a95d4ea6c3fa1b9f173499a95e4"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "title": "epacems-1996-3.zip",
            "parts": {
                "year_quarter": "1996q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 20276462,
            "hash": "2c23b7f09dc8c92a7a45e80502aef588"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-4.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-4.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-4.zip",
            "title": "epacems-1996-4.zip",
            "parts": {
                "year_quarter": "1996q4"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18399321,
            "hash": "52fb801ac36591a3a4a9cd0d60d1842b"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1997-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "title": "epacems-1997-2.zip",
            "parts": {
                "year_quarter": "1997q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 68831002,
            "hash": "9ea4ff4d3176f4f5c7fadd2abf649fa8"
        },
    ],
    "created": "2023-11-30 20:51:43.255388",
    "version": "1",
}))
fake_new_datapackage_fail2 = DataPackage.model_validate_json(json.dumps({
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
            #"orcid": null
        }
    ],
    "sources": [{"blah": "blah"}],
    "profile": "data-package",
    "homepage": "https://catalyst.coop/pudl/",
    "licenses": [
        {
            "name": "other-pd",
            "title": "U.S. Government Works",
            "path": "https://www.usa.gov/publicdomain/label/1.0/"
        }
    ],
    "resources": [
        {
            "profile": "data-resource",
            "name": "epacems-1995-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-1.zip",
            "title": "epacems-1995-1.zip",
            "parts": {
                "year_quarter": "1995q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18392692,
            "hash": "f4ab7fbaa673fa7592feb6977cc41611"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-2.zip",
            "title": "epacems-1995-2.zip",
            "parts": {
                "year_quarter": "1995q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18921381,
            "hash": "fb35601de4e849a6cd123e32f59623f9"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-3.zip",
            "title": "epacems-1995-3.zip",
            "parts": {
                "year_quarter": "1995q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 21734463,
            "hash": "e9ece6bb8190e14d44834bf0a068ac5d"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1995-4.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1995-4.zip",
            "title": "epacems-1995-4.zip",
            "parts": {
                "year_quarter": "1995q4"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 19070179,
            "hash": "c3ca1f2095564d4d7089c5d3d26e5fed"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-1.zip",
            "title": "epacems-1996-1.zip",
            "parts": {
                "year_quarter": "1996q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18886451,
            "hash": "d6e255f46a597b3087019752885bb9b7"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-2.zip",
            "title": "epacems-1996-2.zip",
            "parts": {
                "year_quarter": "1996q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 18397735,
            "hash": "129c4a95d4ea6c3fa1b9f173499a95e4"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1996-3.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1996-3.zip",
            "title": "epacems-1996-3.zip",
            "parts": {
                "year_quarter": "1996q3"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 20276462,
            "hash": "2c23b7f09dc8c92a7a45e80502aef588"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1997-1.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1997-1.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997-1.zip",
            "title": "epacems-1997-1.zip",
            "parts": {
                "year_quarter": "1997q1"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 67290767,
            "hash": "fd263d6ce405419aa5d6e4d14c452739"
        },
        {
            "profile": "data-resource",
            "name": "epacems-1997-2.zip",
            "path": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "remote_url": "https://zenodo.org/records/10306114/files/epacems-1997-2.zip",
            "title": "epacems-1997-2.zip",
            "parts": {
                "year_quarter": "1997q2"
            },
            "encoding": "utf-8",
            "mediatype": "application/zip",
            "format": ".zip",
            "bytes": 68831002,
            "hash": "9ea4ff4d3176f4f5c7fadd2abf649fa8"
        },
    ],
    "created": "2023-11-30 20:51:43.255388",
    "version": "1",
}))

fake_resource = {"epacems:": ResourceInfo(local_path="/blah/blah", partitions={})}

@pytest.mark.parametrize(
    "baseline_datapackage,new_datapackage,resources,success",
    [
        (
            fake_baseline_datapackage,fake_new_datapackage_success,fake_resource,True
        ),
        (
            fake_baseline_datapackage,fake_new_datapackage_fail1,fake_resource,False
        ),
        (
            fake_baseline_datapackage,fake_new_datapackage_fail2,fake_resource,False
        ),
    ]
)
def test_dataset_validate_archive(baseline_datapackage, new_datapackage, resources, success):
    """Test the dataset archiving valiation for epacems."""
    archiver = EpaCemsArchiver("mock_session")
    validation = archiver.dataset_validate_archive(baseline_datapackage, new_datapackage, resources)
    if validation["success"] != success:
        raise AssertionError(f"Expected test success to be {success} but it was {validation['success']}.")

"""Unit test configuration."""

import os
from pathlib import Path

import pytest

from pudl_archiver.metadata.constants import LICENSES

# Use the checked-in fixture descriptor so unit tests never fetch from S3.
os.environ.setdefault(
    "PUDL_DATAPACKAGE_PATH",
    str(Path(__file__).parent.parent / "fixtures" / "datapackage.json"),
)


@pytest.fixture
def fake_dataset(mocker):
    """Make 'pudl_test' a dataset that can be archived, without a real data source."""
    mocker.patch(
        "pudl_archiver.frictionless.get_pudl_sources",
        return_value={
            "pudl_test": {
                "name": "pudl_test",
                "title": "Pudl Test",
                "description": "Test dataset",
                "path": "https://fake.link",
                "license_raw": LICENSES["cc-by-4.0"],
                "contributors": [
                    {
                        "title": "Catalyst Cooperative",
                        "organization": "Catalyst Cooperative",
                    }
                ],
            }
        },
    )

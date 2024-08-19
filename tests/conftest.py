"""Pytest configuration module."""

from datetime import datetime

import pytest

from pudl_archiver.frictionless import DataPackage


@pytest.fixture()
def datapackage():
    """Create test datapackage descriptor."""
    return DataPackage(
        name="pudl_test",
        title="PUDL Test",
        description="Test dataset for the sandbox, thanks!",
        keywords=[],
        contributors=[],
        sources=[],
        licenses=[],
        resources=[],
        created=str(datetime.now()),
        version="1.0.0",
    )

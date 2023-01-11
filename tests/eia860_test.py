"""Test eia860 arhciver."""
import pytest
import pytest_async

from pudl_archiver.archivers.eia860 import Eia860Archiver


@pytest.fixture(scope="module", name="archiver")
def archiver():
    return Eia860Archiver()


def test_get_resources(archiver, mocker):
    """Test get_resources link extraction."""
    pass


def test_get_year_resource(archiver, mocker):
    """Test get_resources link extraction."""
    download_zipfile = mocker.MagicMock(name="download")
    mocker.patch("

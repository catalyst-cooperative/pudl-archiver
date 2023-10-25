"""Test zenodo API client."""
import pydantic
import pytest

from pudl_archiver.orchestrator import DatasetSettings


def test_dataset_settings():
    dataset_settings = DatasetSettings(
        production_doi="10.5281/zenodo.123456",
        sandbox_doi="10.5281/zenodo.1234567",
    )

    assert dataset_settings.production_doi == "10.5281/zenodo.123456"
    assert dataset_settings.sandbox_doi == "10.5281/zenodo.1234567"

    with pytest.raises(pydantic.ValidationError):
        dataset_settings = DatasetSettings(
            sandbox_doi="10.5281/zenodo.1234",
            production_doi="10.5072/zenodo.1234567",
        )

    with pytest.raises(pydantic.ValidationError):
        dataset_settings = DatasetSettings(
            sandbox_doi="random string",
            production_doi="other random string",
        )

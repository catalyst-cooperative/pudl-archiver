"""Test zenodo entities."""
import pytest
from pudl_archiver.zenodo.entities import DepositionMetadata, FileLinks


def test_depo_metadata_from_data_source():
    """Test creating DepositionMetadata from datasource."""
    depo_metadata = DepositionMetadata.from_data_source("ferc1")

    assert depo_metadata.title.startswith("PUDL Raw FERC Form 1")


@pytest.mark.parametrize(
    "url,canonical",
    [
        pytest.param(
            "https://sandbox.zenodo.org/api/records/405/draft/files/unchanged_file.txt/content",
            "https://sandbox.zenodo.org/records/405/files/unchanged_file.txt",
            id="draft",
        ),
        pytest.param(
            "https://sandbox.zenodo.org/api/records/405/files/unchanged_file.txt/content",
            "https://sandbox.zenodo.org/records/405/files/unchanged_file.txt",
            id="no_draft",
        ),
        pytest.param(
            "http://sandbox.zenodo.org/api/records/405/files/unchanged_file.txt/content",
            "http://sandbox.zenodo.org/records/405/files/unchanged_file.txt",
            id="no_https",
        ),
        pytest.param(
            "http://www.zenodo.org/api/records/405/files/unchanged_file.txt/content",
            "http://www.zenodo.org/records/405/files/unchanged_file.txt",
            id="not_sandbox",
        ),
        pytest.param(
            "http://www.zenodo.org/records/405/files/unchanged_file.txt",
            "http://www.zenodo.org/records/405/files/unchanged_file.txt",
            id="already_canonical",
        ),
    ],
)
def test_canonical_url(url, canonical):
    links = FileLinks(download=url)
    assert links.canonical == canonical

"""Test zenodo entities."""
from pudl_archiver.zenodo.entities import DepositionMetadata


def test_depo_metadata_from_data_source():
    """Test creating DepositionMetadata from datasource."""
    depo_metadata = DepositionMetadata.from_data_source("ferc1")

    assert depo_metadata.title.startswith("PUDL Raw FERC Form 1")

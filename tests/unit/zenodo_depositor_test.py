"""Test ZenodoDraftDeposition datapackage generation."""

import datetime

import pytest

from pudl_archiver.depositors.zenodo.depositor import (
    ZenodoAPIClient,
    ZenodoDraftDeposition,
)
from pudl_archiver.depositors.zenodo.entities import (
    Deposition,
    DepositionCreator,
    DepositionLinks,
    DepositionMetadata,
)
from pudl_archiver.utils import RunSettings


def _draft(*, sandbox: bool, record_id: int) -> ZenodoDraftDeposition:
    metadata = DepositionMetadata(
        title="PUDL Raw FERC EQR",
        creators=[DepositionCreator(name="Catalyst Cooperative")],
        description="Description",
        license="cc-by-4.0",
        version="1.0.0",
    )
    now = datetime.datetime.now(tz=datetime.UTC)
    deposition = Deposition(
        conceptrecid=str(record_id),
        created=now,
        id=record_id,
        metadata=metadata,
        modified=now,
        links=DepositionLinks(),
        owner=1,
        record_id=record_id,
        state="unsubmitted",
        submitted=False,
        title=metadata.title,
    )
    return ZenodoDraftDeposition(
        dataset_id="ferceqr",
        settings=RunSettings(depositor="zenodo"),
        api_client=ZenodoAPIClient(sandbox=sandbox),
        deposition=deposition,
    )


@pytest.mark.parametrize(
    "sandbox,record_id,expected_doi",
    [
        (False, 123, "https://doi.org/10.5281/zenodo.123"),
        (True, 456, "https://doi.org/10.5072/zenodo.456"),
    ],
    ids=["production", "sandbox"],
)
def test_generate_datapackage_stamps_reserved_doi(sandbox, record_id, expected_doi):
    draft = _draft(sandbox=sandbox, record_id=record_id)

    datapackage = draft.generate_datapackage({})

    assert datapackage.id_ == expected_doi

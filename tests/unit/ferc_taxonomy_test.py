"""Test how the FERC XBRL archiver loads and archives taxonomies."""

import zipfile

import pytest

from pudl_archiver.archivers.ferc import xbrl
from pudl_archiver.archivers.ferc.xbrl import TaxonomyLoadError, _load_taxonomy

SCHEMA = """<?xml version="1.0" encoding="utf-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
    targetNamespace="http://example.com/{name}" elementFormDefault="qualified">
  {imports}
</xs:schema>
"""


def write_schema(path, imports=()):
    """Write an XML schema that imports the other schemas named in ``imports``."""
    path.write_text(
        SCHEMA.format(
            name=path.stem,
            imports="\n  ".join(
                f'<xs:import namespace="http://example.com/{name}" '
                f'schemaLocation="{name}.xsd"/>'
                for name in imports
            ),
        )
    )
    return path


def test_load_taxonomy_loads_every_file(tmp_path):
    write_schema(tmp_path / "part.xsd")
    entry = write_schema(tmp_path / "entry.xsd", imports=["part"])

    taxonomy = _load_taxonomy(str(entry))

    assert {url.rsplit("/", 1)[-1] for url in taxonomy.urlDocs} >= {
        "entry.xsd",
        "part.xsd",
    }


def test_load_taxonomy_raises_if_a_file_cannot_be_loaded(tmp_path):
    # Arelle logs "File can not be loaded" and carries on without the file
    entry = write_schema(tmp_path / "entry.xsd", imports=["missing"])

    with pytest.raises(TaxonomyLoadError, match=r"1 file\(s\).*missing\.xsd"):
        _load_taxonomy(str(entry))


class FakeTaxonomy:
    """Stand-in for a loaded taxonomy, listing the files that make it up."""

    def __init__(self, urls):
        self.urlDocs = dict.fromkeys(urls)


@pytest.mark.asyncio
async def test_archive_taxonomies_retries_a_taxonomy_that_failed_to_load(
    mocker, tmp_path
):
    entry = "https://ecollection.ferc.gov/taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"
    load = mocker.patch.object(
        xbrl,
        "_load_taxonomy",
        side_effect=[TaxonomyLoadError("a file failed"), FakeTaxonomy([entry])],
    )
    mocker.patch("pudl_archiver.utils.asyncio.sleep")
    response = mocker.Mock()
    response.content.read = mocker.AsyncMock(return_value=b"<xs:schema/>")
    session = mocker.Mock(get=mocker.AsyncMock(return_value=response))

    resource = await xbrl.archive_taxonomies(
        {entry}, xbrl.FercForm.FORM_1, tmp_path, session
    )

    assert load.call_count == 2
    session.get.assert_awaited_once_with(entry, raise_for_status=True)
    with zipfile.ZipFile(resource.local_path) as archive:
        assert archive.namelist() == ["form-1-2022-01-01.zip"]

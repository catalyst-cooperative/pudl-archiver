"""Test how the FERC XBRL archiver loads and archives taxonomies."""

import logging
import zipfile

import pytest

from pudl_archiver.archivers.ferc import xbrl
from pudl_archiver.archivers.ferc.xbrl import (
    TaxonomyLoadError,
    _is_ferc_taxonomy_url,
    _load_taxonomy,
)

ENTRY = "https://ecollection.ferc.gov/taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"

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


@pytest.mark.parametrize(
    "url,expected",
    [
        (ENTRY, True),
        ("https://ECOLLECTION.FERC.GOV/taxonomy/form1/x.xsd", True),
        ("https://ecollection.ferc.gov:443/taxonomy/form1/x.xsd", True),
        ("http://ecollection.ferc.gov/taxonomy/form1/x.xsd", False),
        ("https://ecollection.ferc.gov/api/rssfeed", False),
        ("https://evilferc.gov/taxonomy/form1/x.xsd", False),
        ("https://ecollection.ferc.gov.example.com/taxonomy/form1/x.xsd", False),
        ("https://user@evil.example/taxonomy/form1/x.xsd", False),
        ("http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd", False),
        ("file:///taxonomy/form1/entry.xsd", False),
    ],
)
def test_is_ferc_taxonomy_url(url, expected):
    assert _is_ferc_taxonomy_url(url) is expected


@pytest.fixture
def local_files_are_ferc(mocker):
    """Treat the local test schemas as FERC taxonomy files."""
    mocker.patch.object(xbrl, "_is_ferc_taxonomy_url", return_value=True)


@pytest.fixture
def closes(mocker):
    """Record when Arelle's controllers and models are closed."""
    return {
        "cntlr": mocker.spy(xbrl.Cntlr.Cntlr, "close"),
        "model": mocker.spy(xbrl.ModelXbrl.ModelXbrl, "close"),
    }


def test_load_taxonomy_loads_every_file(tmp_path, local_files_are_ferc, closes):
    write_schema(tmp_path / "part.xsd")
    entry = write_schema(tmp_path / "entry.xsd", imports=["part"])

    with _load_taxonomy(str(entry)) as taxonomy:
        assert {url.rsplit("/", 1)[-1] for url in taxonomy.urlDocs} >= {
            "entry.xsd",
            "part.xsd",
        }
        assert closes["cntlr"].call_count == closes["model"].call_count == 0

    # Leaving the block closes both the taxonomy and its controller
    assert closes["cntlr"].call_count == closes["model"].call_count == 1


def test_load_taxonomy_raises_if_a_file_cannot_be_loaded(
    tmp_path, local_files_are_ferc, closes
):
    # Arelle logs "File can not be loaded" and carries on without the file
    entry = write_schema(tmp_path / "entry.xsd", imports=["missing"])

    with pytest.raises(TaxonomyLoadError, match=r"1 file\(s\).*missing\.xsd"):
        _load_taxonomy(str(entry))

    # The failed attempt closed what it opened
    assert closes["cntlr"].call_count == closes["model"].call_count == 1


def test_load_taxonomy_ignores_unloadable_files_from_other_hosts(tmp_path, mocker):
    # Only FERC's own files are archived, so a missing generic file doesn't matter
    mocker.patch.object(xbrl, "_is_ferc_taxonomy_url", return_value=False)
    entry = write_schema(tmp_path / "entry.xsd", imports=["missing"])

    with _load_taxonomy(str(entry)) as taxonomy:
        assert any(url.endswith("entry.xsd") for url in taxonomy.urlDocs)


def test_failed_attempts_do_not_accumulate_log_handlers(tmp_path, local_files_are_ferc):
    entry = write_schema(tmp_path / "entry.xsd", imports=["missing"])
    arelle_logger = logging.getLogger("arelle")
    handlers_before = list(arelle_logger.handlers)

    for _ in range(3):
        with pytest.raises(TaxonomyLoadError):
            _load_taxonomy(str(entry))

    assert arelle_logger.handlers == handlers_before


class FakeTaxonomy:
    """Stand-in for a loaded taxonomy, listing the files that make it up."""

    def __init__(self, urls):
        self.urlDocs = dict.fromkeys(urls)
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.closed = True


@pytest.mark.asyncio
async def test_archive_taxonomies_retries_a_taxonomy_that_failed_to_load(
    mocker, tmp_path
):
    entry = ENTRY
    taxonomy = FakeTaxonomy([entry])
    load = mocker.patch.object(
        xbrl,
        "_load_taxonomy",
        side_effect=[TaxonomyLoadError("a file failed"), taxonomy],
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
    assert taxonomy.closed
    with zipfile.ZipFile(resource.local_path) as archive:
        assert archive.namelist() == ["form-1-2022-01-01.zip"]


@pytest.mark.asyncio
async def test_archive_taxonomies_skips_and_logs_files_from_other_hosts(
    mocker, tmp_path, caplog
):
    expected = "http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd"
    unexpected = "https://evilferc.gov/taxonomy/form1/x.xsd"
    mocker.patch.object(
        xbrl, "_load_taxonomy", return_value=FakeTaxonomy([ENTRY, expected, unexpected])
    )
    response = mocker.Mock()
    response.content.read = mocker.AsyncMock(return_value=b"<xs:schema/>")
    session = mocker.Mock(get=mocker.AsyncMock(return_value=response))

    with caplog.at_level(logging.WARNING):
        await xbrl.archive_taxonomies({ENTRY}, xbrl.FercForm.FORM_1, tmp_path, session)

    # Only FERC's own file is downloaded
    session.get.assert_awaited_once_with(ENTRY, raise_for_status=True)
    # A host we haven't seen before is logged; the usual XBRL hosts aren't
    assert unexpected in caplog.text
    assert expected not in caplog.text

import json
import zipfile
from collections import defaultdict
from pathlib import Path

import pytest

from pudl_archiver.archivers.ferc.ferc1 import Ferc1Archiver
from pudl_archiver.archivers.ferc.ferc2 import Ferc2Archiver
from pudl_archiver.archivers.ferc.ferc6 import Ferc6Archiver
from pudl_archiver.archivers.ferc.ferc60 import Ferc60Archiver
from pudl_archiver.archivers.ferc.ferc714 import Ferc714Archiver
from pudl_archiver.archivers.ferc.xbrl import (
    TAXONOMY_URL_PATTERN,
    FeedEntry,
    FercForm,
    IndexedFilings,
    archive_year,
)

FERC_FORM_CLASS_LOOKUP = {
    "ferc1": (FercForm.FORM_1, Ferc1Archiver),
    "ferc2": (FercForm.FORM_2, Ferc2Archiver),
    "ferc6": (FercForm.FORM_6, Ferc6Archiver),
    "ferc60": (FercForm.FORM_60, Ferc60Archiver),
    "ferc714": (FercForm.FORM_714, Ferc714Archiver),
}


def make_testdata(module_test_spec_dict):
    testdata = [
        (name, spec) for name, specs in module_test_spec_dict.items() for spec in specs
    ]
    return testdata


valid_years_test_spec_dict = {
    "ferc1": [
        {"years": [1848, 2349], "num_dbf": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1},
        {
            "years": [2003, 2004, 2005, 2021],
            "num_dbf": 4,
        },
    ],
    "ferc2": [
        {"years": [1848, 2349], "num_dbf": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1},
        {
            "years": [2003, 2004, 2005, 2021],
            "num_dbf": 4,
        },
    ],
    "ferc6": [
        {"years": [1848, 2349], "num_dbf": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1},
        {
            "years": [2003, 2004, 2005, 2021],
            "num_dbf": 4,
        },
    ],
    "ferc60": [
        {"years": [1848, 2349], "num_dbf": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 0},
        {
            "years": [2004, 2005, 2006, 2007, 2021],
            "num_dbf": 2,
        },
    ],
    "ferc714": [
        {"years": [1848, 2349], "num_dbf": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 0},
        {
            "years": [2004, 2005, 2006, 2007, 2021],
            "num_dbf": 0,
        },
    ],
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "module_name,test_spec", make_testdata(valid_years_test_spec_dict)
)
async def test_valid_years(module_name, test_spec, mocker):
    form_name, form_class = FERC_FORM_CLASS_LOOKUP[module_name]
    only_years = test_spec["years"]
    num_dbf = test_spec["num_dbf"]

    mocker.patch(
        f"pudl_archiver.archivers.ferc.{module_name}.xbrl.IndexedFilings.index_available_entries",
        lambda: IndexedFilings(
            filings_per_year={
                form_name: {2004: mocker.MagicMock(), 2021: mocker.MagicMock()}
            }
        ),
    )
    mock_session = mocker.AsyncMock()
    archiver = form_class(mock_session, only_years=only_years)
    resources = [res async for res in archiver.get_resources()]
    # don't await these, just check to make sure they have right intention
    dbfs = [r for r in resources if r.__name__ == "get_year_dbf"]
    xbrls = [r for r in resources if r.__name__ == "archive_xbrl_for_form"]
    assert len(dbfs) == num_dbf
    # Always just one method that archives all xbrl resources
    assert len(xbrls) == 1


@pytest.mark.asyncio
async def test_archive_year_metadata(tmpdir):
    """Test format of XBRL archiver metadata."""
    filings = sorted(
        {
            FeedEntry(
                **{
                    "id": "id1",
                    "title": "Filer 1",
                    "summary_detail": {
                        "value": 'href="https://ecollection.ferc.gov/download/filer1.xbrl">www.filer1.xbrl<'
                    },
                    "ferc_formname": FercForm.FORM_1,
                    "published": "Fri, 29 Oct 2021 16:14:44 -0400",
                    "ferc_year": 2021,
                    "ferc_period": "Q4",
                }
            ),
            FeedEntry(
                **{
                    "id": "id2",
                    "title": "Filer 2",
                    "summary_detail": {
                        "value": 'href="https://ecollection.ferc.gov/download/filer2.xbrl">www.filer2.xbrl<'
                    },
                    "published": "Fri, 29 Oct 2021 16:14:44 -0400",
                    "ferc_formname": FercForm.FORM_1,
                    "ferc_year": 2021,
                    "ferc_period": "Q4",
                }
            ),
            FeedEntry(
                **{
                    "id": "id3",
                    "title": "Filer 1",
                    "summary_detail": {
                        "value": 'href="https://ecollection.ferc.gov/download/filer1_v2.xbrl">www.filer1_v2.xbrl<'
                    },
                    "published": "Fri, 29 Oct 2021 18:14:44 -0400",
                    "ferc_formname": FercForm.FORM_1,
                    "ferc_year": 2021,
                    "ferc_period": "Q4",
                }
            ),
            # Duplicated should be removed by set
            FeedEntry(
                **{
                    "id": "id3",
                    "title": "Filer 1",
                    "summary_detail": {
                        "value": 'href="https://ecollection.ferc.gov/download/filer1_v2.xbrl">www.filer1_v2.xbrl<'
                    },
                    "published": "Fri, 29 Oct 2021 16:14:44 -0400",
                    "ferc_formname": FercForm.FORM_1,
                    "ferc_year": 2021,
                    "ferc_period": "Q4",
                }
            ),
        },
        key=lambda e: e.download_url,
    )

    taxonomy_map = {
        str(
            filing.download_url
        ): f"https://ecollection.ferc.gov/taxonomy/form1/2021-01-0{i}/form/form1/form-1_2021-01-0{i}.xsd"
        for i, filing in enumerate(filings)
    }

    expected_metadata = defaultdict(list)
    for filing in filings:
        expected_metadata[f"{filing.title}{filing.ferc_period}"].append(
            {
                "filename": f"{filing.title}_form{filing.ferc_formname.as_int()}_{filing.ferc_period}_{round(filing.published_parsed.timestamp())}.xbrl".replace(
                    " ", "_"
                ),
                "rss_metadata": filing.model_dump(),
                "taxonomy_url": taxonomy_map[str(filing.download_url)],
                "taxonomy_zip_name": f"{TAXONOMY_URL_PATTERN.match(taxonomy_map[str(filing.download_url)]).group(1)}.zip".replace(
                    "_", "-"
                ),
            }
        )

    class FakeResponse:
        def __init__(self, taxonomy_url: str):
            self.taxonomy_url = taxonomy_url

        @property
        def content(self):
            taxonomy_url = self.taxonomy_url

            class Reader:
                async def read(self):
                    return taxonomy_url.encode()

            return Reader()

    class FakeSession:
        async def get(self, url: str, **kwargs):
            return FakeResponse(taxonomy_url=taxonomy_map[url])

    await archive_year(2021, filings, FercForm.FORM_1, Path(tmpdir), FakeSession())

    with (
        zipfile.ZipFile(tmpdir / "ferc1-xbrl-2021.zip", mode="r") as archive,
        archive.open("rssfeed") as f,
    ):
        assert (
            json.dumps(
                {
                    filing_name: expected_metadata[filing_name]
                    for filing_name in sorted(expected_metadata)
                },
                default=str,
                indent=2,
            ).encode()
            == f.read()
        )

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
    archive_year,
)

FERC_FORM_CLASS_LOOKUP = {
    "ferc1": (FercForm.FORM_1, Ferc1Archiver),
    "ferc2": (FercForm.FORM_2, Ferc2Archiver),
    "ferc6": (FercForm.FORM_6, Ferc6Archiver),
    "ferc60": (FercForm.FORM_60, Ferc60Archiver),
    "ferc714": (FercForm.FORM_714, Ferc714Archiver),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ferc_number,valid_years,called_with_years",
    [
        ("1", [1848, 2349], []),
        ("1", [2021, 2022, 2023], [2021]),
        ("1", [2003, 2004, 2005, 2021], [2003, 2004, 2005, 2021]),
        ("2", [1848, 2349], []),
        ("2", [2021, 2022, 2023], [2021]),
        ("2", [2003, 2004, 2005, 2021], [2003, 2004, 2005, 2021]),
        ("6", [1848, 2349], []),
        ("6", [2021, 2022, 2023], [2021]),
        ("6", [2003, 2004, 2005, 2021], [2003, 2004, 2005, 2021]),
        ("60", [1848, 2349], []),
        ("60", [2021, 2022, 2023], []),
        ("60", [2004, 2005, 2006, 2007, 2021], [2006, 2007]),
    ],
)
async def test_valid_years(
    mocker, ferc_number: str, valid_years: list[int], called_with_years: list[int]
):
    form_name, form_class = FERC_FORM_CLASS_LOOKUP[f"ferc{ferc_number}"]
    dbf_mock = mocker.patch(
        f"pudl_archiver.archivers.ferc.ferc{ferc_number}.ferc_online_helpers.get_resources_for_form"
    )
    mocker.patch(
        f"pudl_archiver.archivers.ferc.ferc{ferc_number}.xbrl.archive_xbrl_for_form"
    )
    mock_session = mocker.AsyncMock()
    archiver = form_class(mock_session, only_years=valid_years)
    [r async for r in archiver.get_resources()]
    dbf_mock.assert_called_once_with(
        ferc_form=ferc_number,
        years=called_with_years,
        partitions_base={"data_format": "dbf"},
        download_directory=archiver.download_directory,
    )


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

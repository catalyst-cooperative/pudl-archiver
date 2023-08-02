import pytest

from pudl_archiver.archivers.ferc.ferc1 import Ferc1Archiver
from pudl_archiver.archivers.ferc.ferc2 import Ferc2Archiver
from pudl_archiver.archivers.ferc.ferc6 import Ferc6Archiver
from pudl_archiver.archivers.ferc.ferc60 import Ferc60Archiver
from pudl_archiver.archivers.ferc.ferc714 import Ferc714Archiver
from pudl_archiver.archivers.ferc.xbrl import FercForm

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
        {"years": [1848, 2349], "num_dbf": 0, "num_xbrl": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1, "num_xbrl": 1},
        {"years": [2003, 2004, 2005, 2021], "num_dbf": 4, "num_xbrl": 2},
    ],
    "ferc2": [
        {"years": [1848, 2349], "num_dbf": 0, "num_xbrl": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1, "num_xbrl": 1},
        {"years": [2003, 2004, 2005, 2021], "num_dbf": 4, "num_xbrl": 2},
    ],
    "ferc6": [
        {"years": [1848, 2349], "num_dbf": 0, "num_xbrl": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 1, "num_xbrl": 1},
        {"years": [2003, 2004, 2005, 2021], "num_dbf": 4, "num_xbrl": 2},
    ],
    "ferc60": [
        {"years": [1848, 2349], "num_dbf": 0, "num_xbrl": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 0, "num_xbrl": 1},
        {"years": [2004, 2005, 2006, 2007, 2021], "num_dbf": 2, "num_xbrl": 2},
    ],
    "ferc714": [
        {"years": [1848, 2349], "num_dbf": 0, "num_xbrl": 0},
        {"years": [2021, 2022, 2023], "num_dbf": 0, "num_xbrl": 1},
        {"years": [2004, 2005, 2006, 2007, 2021], "num_dbf": 0, "num_xbrl": 2},
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
    num_xbrl = test_spec["num_xbrl"]

    mocker.patch(
        f"pudl_archiver.archivers.ferc.{module_name}.xbrl.index_available_entries",
        lambda: {form_name: {2004: mocker.MagicMock(), 2021: mocker.MagicMock()}},
    )
    mock_session = mocker.AsyncMock()
    archiver = form_class(mock_session, only_years=only_years)
    resources = [res async for res in archiver.get_resources()]
    # don't await these, just check to make sure they have right intention
    dbfs = [r for r in resources if r.__name__ == "get_year_dbf"]
    xbrls = [r for r in resources if r.__name__ == "get_year_xbrl"]
    assert len(dbfs) == num_dbf
    assert len(xbrls) == num_xbrl

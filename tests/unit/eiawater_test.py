import pytest
from pudl_archiver.archivers.eia.eiawater import EiaWaterArchiver


@pytest.mark.asyncio
async def test_eiawater_filter_years(mocker):
    mock_session = mocker.AsyncMock()
    urls = [
        f"https://www.eia.gov/electricity/data/water/xls/Cooling_Boiler_Generator_Data_Summary_{y}.xlsx"
        for y in range(2000, 2023)
    ]
    get_hyperlinks = mocker.AsyncMock(return_value=urls)
    mocker.patch(
        "pudl_archiver.archivers.eia.eiawater.EiaWaterArchiver.get_hyperlinks",
        get_hyperlinks,
    )
    archiver = EiaWaterArchiver(mock_session, only_years=[2019, 2022])
    resources = [res async for res in archiver.get_resources()]
    assert len(resources) == 2

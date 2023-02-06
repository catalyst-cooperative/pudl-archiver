import pytest
from aiohttp import ClientError

from pudl_archiver.archivers.ferc.xbrl import _get_with_retries


@pytest.mark.asyncio
async def test_retries(mocker):
    session_mock = mocker.Mock(name="session_mock")
    sleep_mock = mocker.AsyncMock()
    mocker.patch("asyncio.sleep", sleep_mock)
    session_mock.get = mocker.AsyncMock(side_effect=ClientError("test error"))

    with pytest.raises(ClientError):
        await _get_with_retries(session_mock, "foo")

    assert session_mock.get.call_count == 5
    assert sleep_mock.call_count == 4
    sleep_mock.assert_has_calls([mocker.call(x) for x in [2, 4, 8, 16]])

from asyncio import to_thread

import pytest

from pudl_archiver.utils import retry_async


@pytest.mark.asyncio
async def test_retry_async(mocker):
    sleep_mock = mocker.AsyncMock()
    mocker.patch("asyncio.sleep", sleep_mock)

    action_mock = mocker.Mock(side_effect=RuntimeError("fuhgeddaboutit"))

    with pytest.raises(RuntimeError):
        await retry_async(lambda: to_thread(action_mock), retry_on=(RuntimeError,))

    assert action_mock.call_count == 5
    assert sleep_mock.call_count == 4

    action_mock.reset_mock()
    sleep_mock.reset_mock()

    with pytest.raises(RuntimeError):
        await retry_async(lambda: to_thread(action_mock), retry_on=(ValueError,))

    assert action_mock.call_count == 1
    assert sleep_mock.call_count == 0

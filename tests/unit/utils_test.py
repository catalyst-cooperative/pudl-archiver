import hashlib
import time
import zipfile
from asyncio import to_thread

import pytest
from pudl_archiver.utils import add_to_archive_stable_hash, retry_async


@pytest.mark.asyncio
async def test_retry_async(mocker):
    sleep_mock = mocker.AsyncMock()
    mocker.patch("asyncio.sleep", sleep_mock)

    action_mock = mocker.Mock(side_effect=RuntimeError("fuhgeddaboutit"))

    with pytest.raises(RuntimeError):
        await retry_async(
            to_thread, args=[action_mock], retry_count=5, retry_on=(RuntimeError,)
        )

    assert action_mock.call_count == 5
    assert sleep_mock.call_count == 4

    action_mock.reset_mock()
    sleep_mock.reset_mock()

    with pytest.raises(RuntimeError):
        await retry_async(to_thread, args=[action_mock], retry_on=(ValueError,))

    assert action_mock.call_count == 1
    assert sleep_mock.call_count == 0


def test_stable_zip_hash(tmp_path):
    a_archive = tmp_path / "a.zip"
    b_archive = tmp_path / "b.zip"

    file_contents = "Call me Ishmael."

    def write_get_digest(archive, filename, data):
        with zipfile.ZipFile(archive, "w") as f:
            add_to_archive_stable_hash(f, filename=filename, data=data)
        with archive.open("rb") as f:
            digest = hashlib.file_digest(f, "md5")
        return digest.hexdigest()

    a_digest = write_get_digest(a_archive, "file1", file_contents)

    # ZipInfo has time resolution of 1s; we could patch out "datetime.now()",
    # but we don't know if that is what ZipFile.writestr() uses under the hood
    # to set the default timestamp. So unfortunately we must sleep for a whole
    # second here.
    time.sleep(1)

    b_digest = write_get_digest(b_archive, "file1", file_contents)

    assert a_digest == b_digest

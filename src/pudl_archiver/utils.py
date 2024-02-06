"""Utility functions used in multiple places within PUDL archiver."""
import asyncio
import logging
import zipfile
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import aiohttp
from pydantic import AnyHttpUrl
from pydantic.functional_serializers import PlainSerializer

logger = logging.getLogger(f"catalystcoop.{__name__}")


# A custom type that wraps AnyHttpUrl, but nicely serializes the URL as a string
Url = Annotated[AnyHttpUrl, PlainSerializer(lambda url: str(url), return_type=str)]


async def retry_async(
    async_func: Callable[..., Awaitable[Any]],
    args: list | None = None,
    kwargs: dict | None = None,
    retry_count: int = 7,
    retry_base_s: int = 2,
    retry_on: tuple[type[Exception], ...] = (aiohttp.ClientError, asyncio.TimeoutError),
):
    """Retry a function that returns a coroutine, with exponential backoff.

    Args:
        async_func: the function to retry.
        args: a list of args to pass in to the retried function.
        kwargs: a dictionary of kwargs to pass into the retried function.
        retry_count: how many total tries to make.
        retry_base_s: how many seconds to wait the first time we retry.
        retry_on: the Exception subclasses to retry on. Defaults to ClientError
            and TimeoutError because that's what aiohttp.ClientSession.request
            and the various .read()/.text() methods on aiohttp.ClientResponse
            can throw.
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    for try_count in range(1, retry_count + 1):  # noqa: RET503
        # try count is 1 indexed for logging clarity
        coro = async_func(*args, **kwargs)
        try:
            return await coro
        except retry_on as e:
            if try_count == retry_count:
                raise e
            retry_delay_s = retry_base_s * 2 ** (try_count - 1)
            logger.info(
                f"Error while executing {coro} (try #{try_count}, retry in {retry_delay_s}s): {type(e)} - {e}"
            )
            await asyncio.sleep(retry_delay_s)


def add_to_archive_stable_hash(archive: zipfile.ZipFile, filename, data: bytes):
    """Add a file to a ZIP archive in a way that makes the hash deterministic.

    ZIP files include some datetime metadata that changes based on when you add
    the file to the archive. This makes their hashes inherently unstable.

    We set the datetime to the earliest possible ZIP datetime, 1980-01-01 (not
    1970! just a quirk of ZIP) to make the hashes stable.
    """
    info = zipfile.ZipInfo(
        filename=filename,
        # Set fixed date to enable hash comparisons between archives
        date_time=(1980, 1, 1, 0, 0, 0),
    )
    info.compress_type = zipfile.ZIP_DEFLATED
    archive.writestr(info, data)

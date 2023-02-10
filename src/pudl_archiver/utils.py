"""Utility functions used in multiple places within PUDL archiver."""
import asyncio
import logging

import aiohttp

logger = logging.getLogger(f"catalystcoop.{__name__}")


async def retry_async(
    async_func,
    retry_count=5,
    retry_base_s=1,
    retry_on=(aiohttp.ClientError, asyncio.TimeoutError),
):
    """Retry a function that returns a coroutine, with exponential backoff.

    If you have something like asyncio.to_thread() which returns a coroutine,
    wrap it in a lambda.

    Args:
        async_func: the function to retry.
        retry_count: how many total tries to make.
        retry_base_s: how many seconds to wait the first time we retry.
        retry_on: the Exception subclasses to retry on. Defaults to ClientError
            and TimeoutError because that's what aiohttp.ClientSession.request
            and the various .read()/.text() methods on aiohttp.ClientResponse
            can throw.
    """
    for try_count in range(1, retry_count + 1):
        # try count is 1 indexed for logging clarity
        coro = async_func()
        try:
            return await coro
        # aiohttp client can either throw ClientError or TimeoutError
        # see https://github.com/aio-libs/aiohttp/issues/7122
        except retry_on as e:
            if try_count == retry_count:
                raise e
            retry_delay_s = retry_base_s * 2**try_count
            logger.info(
                f"Error while executing {coro} (try #{try_count}, retry in {retry_delay_s}s): {e}"
            )
            await asyncio.sleep(retry_delay_s)

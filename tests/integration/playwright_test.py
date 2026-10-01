"""Smoke test that the browsers our archivers rely on can actually be launched.

In CI the browsers come preinstalled in the Playwright container image, which
must be the same version as the playwright package in pixi.lock. A mismatch shows
up here as a "browser executable doesn't exist" error, instead of in the middle of
an archiver run.
"""

import pytest
from playwright.async_api import async_playwright


@pytest.mark.parametrize("browser_type", ["webkit", "chromium"])
async def test_browser_launches(browser_type):
    async with async_playwright() as pw:
        browser = await getattr(pw, browser_type).launch(headless=True)
        try:
            page = await browser.new_page()
            await page.set_content("<p id='greeting'>hello</p>")
            assert await page.inner_text("#greeting") == "hello"
        finally:
            await browser.close()

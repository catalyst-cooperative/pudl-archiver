"""Archive FERC Central Identifier (CID) data."""

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import Tag
from playwright.async_api import async_playwright

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

SOURCE_URL = (
    "https://data.ferc.gov/company-registration/ferc-company-identifier-listing/"
)
DATASET_API_URL = "https://data.ferc.gov/api/v1/dataset/26/"

DATE_LAST_UPDATED_PATTERN = re.compile(r"(\d{2})\/(\d{2})\/(\d{4})\s+.*")


class FercCIDArchiver(AbstractDatasetArchiver):
    """FERC CID archiver."""

    name = "ferccid"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download FERC CID resources."""
        match = await self.get_last_updated_date(page_url=SOURCE_URL)
        month = int(match.group(1))
        day = int(match.group(2))
        year = int(match.group(3))
        if self.valid_year(year):
            download_path = (
                self.download_directory / f"ferccid-{year}-{month}-{day}.csv"
            )
            yield self.download_cid_csv_by_paging_api(
                SOURCE_URL, download_path=download_path
            )

    async def get_last_updated_date(self, page_url: str) -> str:
        """Get the Data Last Updated date from the FERC data viewer page."""
        soup = await self.get_soup(page_url)
        # Find the element that contains the label text
        label_div = soup.find(
            lambda t: (
                isinstance(t, Tag)
                and t.name == "div"
                and t.get_text(strip=True) == "Data Last Updated"
            )
        )
        if not label_div:
            raise RuntimeError("Couldn't find 'Data Last Updated' label div")

        row = label_div.find_parent("div", class_="row")
        if not row:
            raise RuntimeError("Couldn't find parent row for 'Data Last Updated'")

        # extract all text from row and remove the label itself
        row_text = (
            row.get_text(" ", strip=True).replace("Data Last Updated", "").strip()
        )

        match = DATE_LAST_UPDATED_PATTERN.search(row_text)
        if not match:
            raise RuntimeError(
                f"Couldn't find date-like value near 'Data Last Updated'. Row text was: {row_text!r}"
            )

        return match

    async def download_cid_csv_by_paging_api(
        self,
        page_url: str,
        download_path: Path,
        timeout_ms: int = 60_000,
        page_size: int = 100,
    ) -> ResourceInfo:
        """Make request for CID data from inside the page and write to CSV."""
        columns = [
            "Organization_Name",
            "CID",
            "Program",
            "Company_Website",
            "Address",
            "Address2",
            "City",
            "State",
            "Zip",
        ]

        async with async_playwright() as pw:
            browser = await pw.webkit.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(
                    page_url, wait_until="domcontentloaded", timeout=timeout_ms
                )
                await page.wait_for_load_state("networkidle", timeout=timeout_ms)

                all_rows: list[dict[str, Any]] = []
                start = 0
                total_count: int | None = None

                while True:
                    payload = {
                        "startRow": start,
                        "endRow": start + page_size,
                        "sortModel": [],
                        "filterModel": {},
                        "columns": columns,
                        "castData": [],
                    }
                    # make the request from inside the page
                    result = await page.evaluate(
                        """
    async ({url, payload}) => {
    const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        credentials: "include",
    });
    const text = await resp.text();
    return { status: resp.status, ok: resp.ok, text };
    }
    """,
                        {"url": DATASET_API_URL, "payload": payload},
                    )

                    if not result["ok"]:
                        raise RuntimeError(
                            f"In-page fetch failed: {result['status']} body head: {result['text'][:300]!r}"
                        )

                    data = json.loads(result["text"])
                    rows = data.get("rowData")
                    if rows is None:
                        raise RuntimeError(
                            f"rowData key is not present; unexpected response keys: {list(data.keys())}"
                        )
                    if total_count is None and "totalCount" in data:
                        total_count = int(data.get("totalCount", 0))
                    # check if there no more data left
                    if not rows:
                        break

                    all_rows.extend(rows)
                    start += page_size

                    # Stop once we’ve reached the total
                    if total_count is not None and start >= total_count:
                        break

                df = pd.DataFrame(all_rows)
                # Order columns (and include any extra columns if they appear)
                df = df.reindex(
                    columns=columns + [c for c in df.columns if c not in columns]
                )
                df.to_csv(download_path, index=False, encoding="utf-8")

                return ResourceInfo(local_path=download_path, partitions={})
            finally:
                await page.close()
                await browser.close()

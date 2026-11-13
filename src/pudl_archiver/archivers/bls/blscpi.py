"""Download the Bureau of Labor Statistics (BLS) Consumer Price Index (CPI).

BLS publishes the full history of each CPI survey as a directory of tab-delimited flat
files on its LABSTAT download site, for example
https://download.bls.gov/pub/time.series/cu/. Each directory holds the index values
(``<survey>.data.*``), the definitions needed to read them (``<survey>.series``,
``<survey>.item``, ``<survey>.area`` and so on) and a description of the files
(``<survey>.txt``). We archive every file in each directory, unchanged, as one zip file
per survey.

BLS refuses automated requests that do not identify who is making them, and answers
them with an HTML "Access Denied" page and HTTP 403. The archiver therefore sends the
User-Agent given in the ``BLS_USER_AGENT`` environment variable, which should name the
project and give a contact email address, and stops if the variable is not set.
"""

import os
import re
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlsplit

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)
from pudl_archiver.frictionless import ZipLayout

BASE_URL = "https://download.bls.gov/pub/time.series"
USER_AGENT_VARIABLE = "BLS_USER_AGENT"

SURVEYS = {
    "cu": "CPI for All Urban Consumers (CPI-U)",
    "cw": "CPI for Urban Wage Earners and Clerical Workers (CPI-W)",
    "su": "Chained CPI for All Urban Consumers (C-CPI-U)",
}
"""The CPI surveys to archive, keyed by their BLS survey code."""

REQUIRED_FILES = ["series", "item", "area", "footnote"]
"""Definition files that every CPI survey directory should contain.

The chained CPI directory (``su``) has no ``period`` file, so it is not required.
"""


def user_agent() -> str:
    """Return the User-Agent to send to BLS, from the environment."""
    agent = os.environ.get(USER_AGENT_VARIABLE, "").strip()
    if not agent:
        raise RuntimeError(
            f"Set {USER_AGENT_VARIABLE} to a User-Agent that names the project and "
            "gives a contact email address. BLS refuses requests without one."
        )
    return agent


def survey_url(survey: str) -> str:
    """Return the address of the directory that holds one survey's files."""
    return f"{BASE_URL}/{survey}/"


def survey_files(survey: str, links: dict[str, str]) -> dict[str, str]:
    """Return the survey's files from a directory listing, as {file name: URL}.

    Args:
        survey: BLS survey code, e.g. ``cu``.
        links: hyperlinks from the directory listing, as returned by
            :meth:`AbstractDatasetArchiver.get_hyperlinks`.
    """
    directory = survey_url(survey)
    pattern = re.compile(rf"^{re.escape(survey)}\.[\w.]+$")
    files = {}
    for link in links:
        url = urljoin(directory, link)
        parts = urlsplit(url)
        path = PurePosixPath(parts.path)
        if not pattern.match(path.name):
            continue
        if (
            parts.scheme != "https"
            or parts.netloc != urlsplit(BASE_URL).netloc
            or f"{path.parent}/" != urlsplit(directory).path
        ):
            raise RuntimeError(f"Link {link} in {directory} is outside the directory.")
        files[path.name] = url

    missing = [
        f"{survey}.{name}" for name in REQUIRED_FILES if f"{survey}.{name}" not in files
    ]
    if not any(name.startswith(f"{survey}.data.") for name in files):
        missing.append(f"{survey}.data.*")
    if missing:
        raise RuntimeError(
            f"{directory} does not list {', '.join(missing)}. BLS may have refused "
            "the request, or the directory may have changed."
        )
    return files


def looks_like_html(head: bytes) -> bool:
    """Return True if the start of a file looks like an HTML page."""
    head = head.lstrip().lower()
    return head.startswith((b"<!doctype html", b"<html"))


class BlsCpiArchiver(AbstractDatasetArchiver):
    """BLS Consumer Price Index archiver."""

    name = "blscpi"

    async def get_resources(self) -> ArchiveAwaitable:
        """Yield one resource for each CPI survey."""
        headers = {"User-Agent": user_agent()}
        for survey in SURVEYS:
            yield self.get_survey_resource(survey, headers)

    async def get_survey_resource(
        self, survey: str, headers: dict[str, str]
    ) -> ResourceInfo:
        """Download every file in one survey's directory into a zip file."""
        directory = survey_url(survey)
        links = await self.get_hyperlinks(directory, headers=headers)
        files = survey_files(survey, links)
        self.logger.info(f"Found {len(files)} files in {directory}")

        zip_path = self.download_directory / f"blscpi-{survey}.zip"
        for filename, url in sorted(files.items()):
            download_path = self.download_directory / filename
            status = await self.download_file(url, download_path, headers=headers)
            if status != 200:
                download_path.unlink(missing_ok=True)
                raise RuntimeError(f"{url} returned HTTP {status}.")
            with download_path.open("rb") as f:
                if looks_like_html(f.read(256)):
                    raise RuntimeError(
                        f"{url} returned an HTML page instead of a data file. "
                        "BLS may have refused the request."
                    )
            with download_path.open("rb") as f:
                self.add_to_archive(zip_path=zip_path, filename=filename, blob=f)
            download_path.unlink()

        return ResourceInfo(
            local_path=zip_path,
            partitions={"survey": survey},
            layout=ZipLayout(file_paths=set(files)),
        )

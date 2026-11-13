import zipfile

import pytest

from pudl_archiver.archivers.bls.blscpi import (
    SURVEYS,
    USER_AGENT_VARIABLE,
    BlsCpiArchiver,
    looks_like_html,
    survey_files,
    user_agent,
)

AGENT = "pudl-archiver (contact@example.com)"


def listing(survey: str, names: list[str]) -> dict[str, str]:
    """Build directory-listing links shaped like the BLS LABSTAT site's."""
    links = {"/pub/time.series/": "[To Parent Directory]"}
    links |= {f"/pub/time.series/{survey}/{name}": name for name in names}
    return links


CU_FILES = [
    "cu.area",
    "cu.data.0.Current",
    "cu.data.1.AllItems",
    "cu.footnote",
    "cu.item",
    "cu.period",
    "cu.series",
    "cu.txt",
]


def test_user_agent_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv(USER_AGENT_VARIABLE, f"  {AGENT} ")
    assert user_agent() == AGENT


@pytest.mark.parametrize("value", [None, "", "   "])
def test_user_agent_is_required(monkeypatch, value):
    if value is None:
        monkeypatch.delenv(USER_AGENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(USER_AGENT_VARIABLE, value)
    with pytest.raises(RuntimeError, match=f"Set {USER_AGENT_VARIABLE}"):
        user_agent()


def test_survey_files_keeps_only_the_survey_files():
    links = listing("cu", CU_FILES)
    files = survey_files("cu", links)
    assert set(files) == set(CU_FILES)
    assert files["cu.series"] == "https://download.bls.gov/pub/time.series/cu/cu.series"


def test_survey_files_does_not_need_a_period_file():
    # The chained CPI directory has no su.period.
    names = ["su.area", "su.data.0.Current", "su.footnote", "su.item", "su.series"]
    assert set(survey_files("su", listing("su", names))) == set(names)


def test_survey_files_rejects_a_listing_with_missing_files():
    links = listing("cu", ["cu.area", "cu.item", "cu.txt"])
    with pytest.raises(RuntimeError, match=r"cu\.series.*cu\.data\.\*"):
        survey_files("cu", links)


def test_survey_files_rejects_an_access_denied_page():
    # BLS's refusal page has no links to the survey's files.
    with pytest.raises(RuntimeError, match="BLS may have refused the request"):
        survey_files("cu", {})


@pytest.mark.parametrize(
    "link",
    [
        "https://example.com/pub/time.series/cu/cu.series",
        "http://download.bls.gov/pub/time.series/cu/cu.series",
        "/pub/time.series/cw/cu.series",
    ],
)
def test_survey_files_rejects_links_outside_the_directory(link):
    links = listing("cu", CU_FILES) | {link: "cu.series"}
    with pytest.raises(RuntimeError, match="outside the directory"):
        survey_files("cu", links)


def test_looks_like_html():
    assert looks_like_html(b"\r\n<!DOCTYPE HTML PUBLIC")
    assert looks_like_html(b"<html><head><title>Access Denied</title>")
    assert not looks_like_html(b"series_id        \tyear\tperiod\tvalue")


@pytest.mark.asyncio
async def test_get_resources_requires_a_user_agent(mocker, monkeypatch):
    monkeypatch.delenv(USER_AGENT_VARIABLE, raising=False)
    archiver = BlsCpiArchiver(mocker.AsyncMock())
    with pytest.raises(RuntimeError, match=f"Set {USER_AGENT_VARIABLE}"):
        [res async for res in archiver.get_resources()]


@pytest.mark.asyncio
async def test_get_resources_yields_one_resource_per_survey(mocker, monkeypatch):
    monkeypatch.setenv(USER_AGENT_VARIABLE, AGENT)
    archiver = BlsCpiArchiver(mocker.AsyncMock())
    get_survey_resource = mocker.patch.object(
        archiver, "get_survey_resource", mocker.AsyncMock(return_value="resource")
    )

    resources = [res async for res in archiver.get_resources()]
    for res in resources:
        await res

    assert [call.args for call in get_survey_resource.call_args_list] == [
        (survey, {"User-Agent": AGENT}) for survey in SURVEYS
    ]


def fake_download(contents: dict[str, bytes], status: int = 200):
    """Write each file's contents to disk, as download_file would."""

    async def download_file(url, path, **kwargs):
        path.write_bytes(contents[url.rsplit("/", 1)[-1]])
        return status

    return download_file


@pytest.mark.asyncio
async def test_get_survey_resource_zips_every_file_unchanged(mocker):
    archiver = BlsCpiArchiver(mocker.AsyncMock())
    headers = {"User-Agent": AGENT}
    contents = {name: f"contents of {name}\n".encode() for name in CU_FILES}
    get_hyperlinks = mocker.patch.object(
        archiver, "get_hyperlinks", return_value=listing("cu", CU_FILES)
    )
    download = mocker.patch.object(
        archiver, "download_file", side_effect=fake_download(contents)
    )

    resource = await archiver.get_survey_resource("cu", headers)

    get_hyperlinks.assert_awaited_once_with(
        "https://download.bls.gov/pub/time.series/cu/", headers=headers
    )
    assert all(call.kwargs == {"headers": headers} for call in download.call_args_list)
    assert resource.partitions == {"survey": "cu"}
    assert resource.local_path.name == "blscpi-cu.zip"
    assert {path.name for path in resource.layout.file_paths} == set(CU_FILES)
    with zipfile.ZipFile(resource.local_path) as archive:
        assert {name: archive.read(name) for name in archive.namelist()} == contents
    # Downloads are removed once they are in the zip file.
    assert not any((archiver.download_directory / name).exists() for name in CU_FILES)


@pytest.mark.asyncio
async def test_get_survey_resource_raises_on_http_error(mocker):
    archiver = BlsCpiArchiver(mocker.AsyncMock())
    mocker.patch.object(
        archiver, "get_hyperlinks", return_value=listing("cu", CU_FILES)
    )
    contents = dict.fromkeys(CU_FILES, b"Access Denied")
    mocker.patch.object(
        archiver, "download_file", side_effect=fake_download(contents, status=403)
    )

    with pytest.raises(RuntimeError, match="returned HTTP 403"):
        await archiver.get_survey_resource("cu", {"User-Agent": AGENT})


@pytest.mark.asyncio
async def test_get_survey_resource_raises_on_html_instead_of_data(mocker):
    archiver = BlsCpiArchiver(mocker.AsyncMock())
    mocker.patch.object(
        archiver, "get_hyperlinks", return_value=listing("cu", CU_FILES)
    )
    contents = dict.fromkeys(CU_FILES, b"<html><title>Access Denied</title></html>")
    mocker.patch.object(archiver, "download_file", side_effect=fake_download(contents))

    with pytest.raises(RuntimeError, match="HTML page instead of a data file"):
        await archiver.get_survey_resource("cu", {"User-Agent": AGENT})

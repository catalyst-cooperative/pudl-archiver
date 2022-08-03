"""Test the FERC XBRL data scraping script."""
import json
from pathlib import Path

from pudl_scrapers.bin import ferc_xbrl

BASE_PATH = Path(__file__).parent

EXPECTED_METADATA = {
    "Example FilerQ4": {
        "0": {
            "id": "0",
            "guidislink": False,
            "links": [
                {"rel": "alternate", "type": "text/html", "href": "https://ferc.gov/"}
            ],
            "link": "https://ferc.gov/",
            "title": "Example Filer",
            "title_detail": {
                "type": "text/plain",
                "language": None,
                "base": "",
                "value": "Example Filer",
            },
            "summary": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_1.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
            "summary_detail": {
                "type": "text/html",
                "language": None,
                "base": "",
                "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_1.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
            },
            "published": "Fri, 27 May 2022 20:20:21 -0400",
            "published_parsed": [2022, 5, 28, 0, 20, 21, 5, 148, 0],
            "updated": "2022-05-25T20:20:21-04:00",
            "updated_parsed": [2022, 5, 26, 0, 20, 21, 3, 146, 0],
            "ferc_filingid": "91925",
            "ferc_cid": "C011405",
            "ferc_cpacertrequired": "False",
            "ferc_cpacertuploaded": "False",
            "ferc_formname": "Form 1",
            "ferc_period": "Q4",
            "ferc_privilegedaccessionnumber": "",
            "ferc_status": "Accepted",
            "ferc_submittedon": "5/25/2022 8:20:21 PM",
            "ferc_year": "2021",
            "ferc_xbrlfile": {
                "file": "XBRL_4_389_20220525171041_91925.html",
                "type": "html_rendering",
                "url": "https://ecollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html",
                "availability": "Public",
            },
            "ferc_xbrlfiling": "",
        },
        "1": {
            "id": "1",
            "guidislink": False,
            "links": [
                {"rel": "alternate", "type": "text/html", "href": "https://ferc.gov/"}
            ],
            "link": "https://ferc.gov/",
            "title": "Example Filer",
            "title_detail": {
                "type": "text/plain",
                "language": None,
                "base": "",
                "value": "Example Filer",
            },
            "summary": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_2.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
            "summary_detail": {
                "type": "text/html",
                "language": None,
                "base": "",
                "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_2.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
            },
            "published": "Wed, 25 May 2022 20:20:21 -0400",
            "published_parsed": [2022, 5, 26, 0, 20, 21, 3, 146, 0],
            "updated": "2022-05-25T20:20:21-04:00",
            "updated_parsed": [2022, 5, 26, 0, 20, 21, 3, 146, 0],
            "ferc_filingid": "91925",
            "ferc_cid": "C011405",
            "ferc_cpacertrequired": "False",
            "ferc_cpacertuploaded": "False",
            "ferc_formname": "Form 1",
            "ferc_period": "Q4",
            "ferc_privilegedaccessionnumber": "",
            "ferc_status": "Accepted",
            "ferc_submittedon": "5/25/2022 8:20:21 PM",
            "ferc_year": "2021",
            "ferc_xbrlfile": {
                "file": "XBRL_4_389_20220525171041_91925.html",
                "type": "html_rendering",
                "url": "https://ecollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html",
                "availability": "Public",
            },
            "ferc_xbrlfiling": "",
        },
    }
}


def test_archive_script(mocker):
    """Test the archiving script."""
    # Prepare mocks
    zipfile_mock = mocker.MagicMock(name="zipfile")
    mocker.patch("pudl_scrapers.bin.ferc_xbrl.zipfile", new=zipfile_mock)

    # Mock out requests to avoid making any real requests
    _ = mocker.Mock("pudl_scrapers.bin.requests")

    # Call function
    ferc_xbrl.archive_filings(
        feed_path=BASE_PATH / "data/ferc_rssfeed.atom",
        form_number=1,
        filter_years=[2021],
        output_dir=Path("./"),
    )

    # Test that zipfile was created with proper name
    zipfile_mock.ZipFile.assert_called_once_with(Path("./") / "ferc1-2021.zip", "w")

    # Get mock associated with ZipFile context manager
    archive_mock = zipfile_mock.ZipFile.return_value.__enter__.return_value

    # Test that all expected filings were written to zip
    archive_mock.open.assert_any_call("0.xbrl", "w")
    archive_mock.open.assert_any_call("1.xbrl", "w")
    archive_mock.open.assert_any_call("rssfeed", "w")

    # Test metadata was written as expected
    archive_mock.open.return_value.__enter__.return_value.write.assert_any_call(
        json.dumps(EXPECTED_METADATA).encode("utf-8")
    )

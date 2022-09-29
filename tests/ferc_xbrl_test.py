"""Test the FERC XBRL data scraping script."""
from pathlib import Path
from zipfile import ZIP_DEFLATED

from pudl_scrapers.bin import ferc_xbrl

BASE_PATH = Path(__file__).parent

FORM1_FILINGS = {
    2021: {
        ferc_xbrl.FeedEntry(
            **{
                "id": "0",
                "title": "Example Filer",
                "summary_detail": {
                    "type": "text/html",
                    "language": None,
                    "base": "",
                    "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_0.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
                },
                "ferc_formname": "Form 1",
                "published_parsed": [2022, 5, 28, 0, 20, 21, 5, 148, 0],
                "ferc_period": "Q4",
                "ferc_year": "2021",
            }
        ),
        ferc_xbrl.FeedEntry(
            **{
                "id": "1",
                "title": "Example Filer",
                "summary_detail": {
                    "type": "text/html",
                    "language": None,
                    "base": "",
                    "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_1.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
                },
                "published_parsed": [2022, 3, 28, 0, 20, 21, 5, 148, 0],
                "ferc_formname": "Form 1",
                "ferc_period": "Q4",
                "ferc_year": "2021",
            }
        ),
        ferc_xbrl.FeedEntry(
            **{
                "id": "2",
                "title": "Example Filer 2",
                "summary_detail": {
                    "type": "text/html",
                    "language": None,
                    "base": "",
                    "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_2.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
                },
                "published_parsed": [2022, 3, 28, 0, 20, 21, 5, 148, 0],
                "ferc_formname": "Form 1",
                "ferc_period": "Q4",
                "ferc_year": "2021",
            }
        ),
    },
    2022: {
        ferc_xbrl.FeedEntry(
            **{
                "id": "3",
                "title": "Example Filer",
                "summary_detail": {
                    "type": "text/html",
                    "language": None,
                    "base": "",
                    "value": 'Form 1<br /> Download Files:<br /><ul><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146659/1?filename=filing_3.xbrl">XBRL_4_389_20220525171041.xbrl</a><br /></li><li><a href="https://eCollection.ferc.gov/api/DownloadDocument/146683/3?filename=XBRL_4_389_20220525171041_91925.html">XBRL_4_389_20220525171041_91925.html</a><br /></li></ul>',
                },
                "published_parsed": [2022, 3, 28, 0, 20, 21, 5, 148, 0],
                "ferc_formname": "Form 1",
                "ferc_period": "Q4",
                "ferc_year": "2022",
            }
        ),
    },
}


def test_archive_filings(mocker):
    """Test archive filings function with multiple years."""
    # Prepare mocks
    zipfile_mock = mocker.MagicMock(name="zipfile")
    mocker.patch("pudl_scrapers.bin.ferc_xbrl.zipfile", new=zipfile_mock)

    new_output_dir_mock = mocker.MagicMock(
        name="new_output_dir", return_value=Path("./")
    )
    mocker.patch("pudl_scrapers.bin.ferc_xbrl.new_output_dir", new=new_output_dir_mock)

    mocker.patch("pudl_scrapers.bin.ferc_xbrl.archive_taxonomy")

    # Mock out requests to avoid making any real requests
    _ = mocker.Mock("pudl_scrapers.bin.ferc_xbrl.requests")

    # Call function
    ferc_xbrl.archive_year(
        year=2021,
        filings=FORM1_FILINGS[2021],
        form=ferc_xbrl.FercForm.FORM_1,
        output_dir=Path("./"),
    )

    # Test that zipfile was created with proper name
    zipfile_mock.ZipFile.assert_any_call(
        Path("./") / "ferc1-xbrl-2021.zip", "w", compression=ZIP_DEFLATED
    )

    # Get mock associated with ZipFile context manager
    archive_mock = zipfile_mock.ZipFile.return_value.__enter__.return_value

    # Test that all expected filings were written to zip
    archive_mock.open.assert_any_call("0.xbrl", "w")
    archive_mock.open.assert_any_call("2.xbrl", "w")
    archive_mock.open.assert_any_call("rssfeed", "w")

    # Call function
    ferc_xbrl.archive_year(
        year=2022,
        filings=FORM1_FILINGS[2022],
        form=ferc_xbrl.FercForm.FORM_1,
        output_dir=Path("./"),
    )

    # Test that zipfile was created with proper name
    zipfile_mock.ZipFile.assert_any_call(
        Path("./") / "ferc1-xbrl-2022.zip", "w", compression=ZIP_DEFLATED
    )

    # Test that all expected filings were written to zip
    archive_mock.open.assert_any_call("3.xbrl", "w")
    archive_mock.open.assert_any_call("rssfeed", "w")

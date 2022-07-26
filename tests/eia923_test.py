"""Test the EIA-923 Spider."""
from pudl_scrapers.spiders.eia923 import Eia923Spider
from tests import factories


class TestEia923:
    """Validate Eia923 Spider."""

    def test_spider_ids_files(self):
        """Eia923 spider parses zip file links."""
        spider = Eia923Spider()
        resp = factories.TestResponseFactory(eia923=True)
        result = list(spider.all_forms(resp))

        assert (
            result[0].url == "https://www.eia.gov/electricity/data/"
            "eia923/xls/f923_2019.zip"
        )
        assert result[0].meta["year"] == 2019

        assert (
            result[-1].url == "https://www.eia.gov/electricity/data/"
            "eia923/archive/xls/f906920_2001.zip"
        )

        assert result[-1].meta["year"] == 2001

    def test_spider_gets_specific_year(self):
        """Eia923 spider can pick forms for a specific year."""
        spider = Eia923Spider()
        resp = factories.TestResponseFactory(eia923=True)

        result = spider.form_for_year(resp, 2007)

        assert result is not None
        assert (
            result.url == "https://www.eia.gov/electricity/data/eia923/"
            "archive/xls/f906920_2007.zip"
        )
        assert result.meta["year"] == 2007

        for year in range(2001, 2019):
            result = spider.form_for_year(resp, year)
            assert result is not None

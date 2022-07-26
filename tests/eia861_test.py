"""Test for the EIA-861 Spider."""
from pudl_scrapers.spiders.eia861 import Eia861Spider
from tests import factories


class TestEia861:
    """Validate Eia861 Spider."""

    def test_spider_ids_files(self):
        """Eia861 spider parses zip file links."""
        spider = Eia861Spider()
        resp = factories.TestResponseFactory(eia861=True)
        result = list(spider.all_forms(resp))

        assert (
            result[0].url == "https://www.eia.gov/electricity/data/"
            "eia861/zip/f8612019.zip"
        )
        assert result[0].meta["year"] == 2019

        assert (
            result[-1].url == "https://www.eia.gov/electricity/data/"
            "eia861/archive/zip/f86190.zip"
        )
        assert result[-1].meta["year"] == 1990

    def test_spider_gets_specific_year(self):
        """Eia861 spider can pick forms for a specific year."""
        spider = Eia861Spider()
        resp = factories.TestResponseFactory(eia861=True)

        # 2011 is a cutoff point for different name scheme
        result = spider.form_for_year(resp, 2011)

        assert result is not None
        assert (
            result.url == "https://www.eia.gov/electricity/data/"
            "eia861/archive/zip/f86111.zip"
        )
        assert result.meta["year"] == 2011

        # 2012 has newer name scheme
        result = spider.form_for_year(resp, 2012)

        assert result is not None
        assert (
            result.url == "https://www.eia.gov/electricity/data/"
            "eia861/archive/zip/f8612012.zip"
        )
        assert result.meta["year"] == 2012

        for year in range(1990, 2020):
            result = spider.form_for_year(resp, year)
            assert result is not None

"""Test EIA 860 M."""

from pudl_scrapers.spiders.eia860m import Eia860MSpider
from tests import factories


class TestEia860M:
    """Validate Eia860M Spider."""

    def test_spider_ids_files(self):
        """EIA 860 M spider parses zip file links."""
        spider = Eia860MSpider()
        resp = factories.TestResponseFactory(eia860m=True)
        result = list(spider.all_forms(resp))

        assert result[0].url == "https://www.eia.gov/electricity/data" \
            "/eia860m/xls/august_generator2020.xlsx"
        assert result[0].meta["year"] == 2020
        assert result[0].meta["month"] == "08"
        assert result[-1].url == "https://www.eia.gov/electricity/data" \
            "/eia860m/archive/xls/july_generator2015.xlsx"
        assert result[-1].meta["year"] == 2015

    def test_spider_gets_specific_year(self):
        """EIA 860 M spider can pick forms for a specific year."""
        spider = Eia860MSpider()
        resp = factories.TestResponseFactory(eia860m=True)
        result = spider.form_for_month_year(resp, "January", 2018)

        assert result is not None
        assert result.url == "https://www.eia.gov/electricity/data/eia860m" \
            "/archive/xls/january_generator2018.xlsx"
        assert result.meta["year"] == 2018

        for year in range(2015, 2021):
            result = spider.form_for_month_year(resp, month="July", year=year)
            assert result is not None

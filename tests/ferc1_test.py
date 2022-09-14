"""Test the FERC 1 spider."""
from pudl_scrapers.spiders.ferc1 import Ferc1Spider


class TestFerc1:
    """Validate Ferc1 Spider."""

    def test_spider_requests_all_files(self):
        """Ferc1 knows all file urls."""
        spider = Ferc1Spider()
        all_forms = list(spider.all_form_requests())

        assert all_forms[0].url == "https://forms.ferc.gov/f1allyears/f1_1994.zip"
        assert all_forms[0].meta["year"] == 1994

        assert all_forms[26].url == "https://forms.ferc.gov/f1allyears/f1_2020.zip"
        assert all_forms[26].meta["year"] == 2020

    def test_spider_gets_specific_year(self):
        """Ferc1 generates any individual form url."""
        spider = Ferc1Spider()

        for year in range(1994, 2022):
            form_req = spider.form_for_year(year)
            assert form_req.url == f"https://forms.ferc.gov/f1allyears/f1_{year}.zip"
            assert form_req.meta["year"] == year

# -*- coding: utf-8 -*-

from pudl_scrapers.spiders.ferc1 import Ferc1Spider


class TestFerc1:
    """Validate Ferc1 Spider"""

    def spider_requests_all_files(self):
        """Ferc1 knows all file urls"""
        spider = Ferc1Spider()
        all_forms = spider.all_form_requests()

        assert all_forms[0].url == "ftp://eforms1.ferc.gov/f1allyears/f1_2018.zip"
        assert all_forms[0].meta["year"] == 2018

        assert all_forms[-1].url == "ftp://eforms1.ferc.gov/f1allyears/" \
                                    "f1_1994.zip"
        assert all_forms[-1].meta["year"] == 1994

    def test_spider_gets_specific_year(self):
        """Ferc1 generates any individual form url"""
        spider = Ferc1Spider()

        for year in range(1994, 2018):
            form_req = spider.form_for_year(year)
            assert form_req.url == \
                "ftp://eforms1.ferc.gov/f1allyears/f1_%d.zip" % year
            assert form_req.meta["year"] == year

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from spiders.eia860 import Eia860Spider
from . import factories


class TestEia860:
    """Validate Eia860 Spider"""

    def test_spider_ids_files(self):
        """Eia860 spider parses zip file links"""
        spider = Eia860Spider()
        resp = factories.TestResponseFactory()
        result = list(spider.all_forms(resp))

        assert result[0].url == "https://www.eia.gov/electricity/data/eia860" \
                                "/xls/eia8602018.zip"
        assert result[0].meta["year"] == 2018
        assert result[-1].url == "https://www.eia.gov/electricity/data" \
                                 "/eia860/archive/xls/eia8602001.zip"
        assert result[-1].meta["year"] == 2001

    def test_spider_gets_specific_year(self):
        """Eia860 spider can pick forms for a specific year"""
        spider = Eia860Spider()
        resp = factories.TestResponseFactory()

        result = spider.form_for_year(resp, 2004)

        assert result is not None
        assert result.url == "https://www.eia.gov/electricity/data/eia860" \
                             "/archive/xls/eia8602004.zip"
        assert result.meta["year"] == 2004

        for year in range(2001, 2018):
            result = spider.form_for_year(resp, year)
            assert result is not None

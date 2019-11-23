# -*- coding: utf-8 -*-

from datetime import date
from pudl.spiders.ipm import IpmSpider
from . import factories


class TestIpm:
    """Validate Ipm Spider"""

    def test_spider_ids_files(self):
        """Ipm spider parses zip file links"""
        spider = IpmSpider()
        resp = factories.TestResponseFactory(ipm=True)
        result = list(spider.parse(resp))

        assert result[0].url == "https://www.epa.gov/sites/production/" \
                                "files/2019-11/needs_v6_09-30-19.xlsx"
        assert result[0].meta["version"] == 6
        assert result[0].meta["revision"] == date(2019, 9, 30)

        assert result[-1].url == "https://www.epa.gov/sites/production/" \
                                 "files/2019-10/needs_v6_initial_run_1.xlsx"
        assert result[-1].meta["version"] == 6
        assert result[-1].meta["revision"] == date(2018, 5, 30)

    def test_needs_version(self):
        """Spider can get the NEEDS version number from a description"""
        spider = IpmSpider()

        for i in range(10):
            description = "NEEDS v%d rev: 5-31-2019" % i
            assert spider.needs_version(description) == i

    def test_needs_revision(self):
        """Spider can get the NEEDS revision from a description"""
        spider = IpmSpider()

        assert spider.needs_revision("NEEDS v6 rev: 5-31-2019") == \
            date(2019, 5, 31)

        assert spider.needs_revision("NEEDS v6 rev: 11-30-2018") == \
            date(2018, 11, 30)

        assert spider.needs_revision("NEEDS v6 rev: 9-30-2019") == \
            date(2019, 9, 30)

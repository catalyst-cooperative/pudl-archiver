# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DataFile(scrapy.Item):
    data = scrapy.Field()  # file binary
    save_path = scrapy.Field(serializer=str)


class Eia860(DataFile):
    """The Eia860 forms in a zip file"""
    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia860(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia861(DataFile):
    """The Eia861 forms in a zip file"""
    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia861(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia923(DataFile):
    """The Eia923 forms in a zip file"""
    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia923(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Ferc1(DataFile):
    """The Ferc1 forms in a zip file"""
    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Ferc1(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Ipm(DataFile):
    """An IPM (NEEDS) xls file"""
    revision = scrapy.Field()
    version = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Ipm(version=%d, revision=%s)" % (
            self["version"], self["revision"].isoformat())

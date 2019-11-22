# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DataFile(scrapy.Item):
    data = scrapy.Field()  # file binary
    year = scrapy.Field(serializer=int)
    save_path = scrapy.Field(serializer=str)


class Eia860(DataFile):
    """The Eia860 forms in a zip file"""
    def __repr__(self):
        return "Eia860(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia861(DataFile):
    """The Eia861 forms in a zip file"""
    def __repr__(self):
        return "Eia861(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia923(DataFile):
    """The Eia923 forms in a zip file"""
    def __repr__(self):
        return "Eia923(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Ferc1(DataFile):
    """The Ferc1 forms in a zip file"""
    def __repr__(self):
        return "Ferc1(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])

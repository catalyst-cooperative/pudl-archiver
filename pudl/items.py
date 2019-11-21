# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EiaFile(scrapy.Item):
    data = scrapy.Field()  # file binary
    year = scrapy.Field(serializer=int)
    save_path = scrapy.Field(serializer=str)


class Eia860(EiaFile):
    """The Eia860 forms in a zip file"""
    def __repr__(self):
        return "Eia860(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia861(EiaFile):
    """The Eia861 forms in a zip file"""
    def __repr__(self):
        return "Eia861(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])


class Eia923(EiaFile):
    """The Eia923 forms in a zip file"""
    def __repr__(self):
        return "Eia923(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])

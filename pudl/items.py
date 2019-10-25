# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Eia860(scrapy.Item):
    """The Eia860 forms in a zip file"""
    data = scrapy.Field()  # file binary
    year = scrapy.Field(serializer=int)
    save_path = scrapy.Field(serializer=str)

    def __repr__(self):
        return "Eia860(year=%d, save_path=%s)" % (
            self["year"], self["save_path"])

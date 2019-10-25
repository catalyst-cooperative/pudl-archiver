# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Eia860(scrapy.Item):
    """The Eia860 forms in a zip file"""
    year = scrapy.Field(serializer=int)
    data = scrapy.Field()  # file binary

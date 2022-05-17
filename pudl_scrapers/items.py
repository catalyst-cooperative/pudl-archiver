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
    """The Eia860 forms in a zip file."""

    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia860(year=%d, save_path='%s')" % (self["year"], self["save_path"])


class Eia860M(DataFile):
    """The EIA 860 M forms in a xlsx file."""

    year = scrapy.Field(serializer=int)
    month = scrapy.Field(serializer=str)

    def __repr__(self):
        return (
            f"Eia860M(year={self['year']}, month={self['month']}, "
            f"save_path={self['save_path']})"
        )


class Eia861(DataFile):
    """The Eia861 forms in a zip file."""

    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia861(year=%d, save_path='%s')" % (self["year"], self["save_path"])


class Eia923(DataFile):
    """The Eia923 forms in a zip file."""

    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Eia923(year=%d, save_path='%s')" % (self["year"], self["save_path"])


class Ferc1(DataFile):
    """The Ferc1 forms in a zip file."""

    year = scrapy.Field(serializer=int)

    def __repr__(self):
        return "Ferc1(year=%d, save_path='%s')" % (self["year"], self["save_path"])


class Ferc714(DataFile):
    """The Ferc714 data zip file."""

    def __repr__(self):
        return "Ferc714('%s')" % self["save_path"]


class EipInfrastructure(DataFile):
    """The EipInfrastructure excel file."""

    def __repr__(self):
        return "EipInfrastructure('%s')" % self["save_path"]


class LblnIsoQueues(DataFile):
    """The LblnIsoQueues excel file."""

    def __repr__(self):
        return "LblnIsoQueues('%s')" % self["save_path"]


class EpaIpm(DataFile):
    """An EPA IPM (NEEDS) xls file."""

    revision = scrapy.Field()
    version = scrapy.Field(serializer=int)

    def __repr__(self):
        return "EpaIpm(version=%d, revision='%s', save_path='%s')" % (
            self["version"],
            self["revision"].isoformat(),
            self["save_path"],
        )


class EpaEiaCrosswalk(DataFile):
    """BLAH."""

    def __repr__(self):
        return blah


class Cems(DataFile):
    """A CEMS zip file."""

    def __repr__(self):
        return "Cems(save_path='%s')" % self["save_path"]


class Census(DataFile):
    """Census zip file."""

    def __repr__(self):
        return "Census(save_path='%s')" % self["save_path"]

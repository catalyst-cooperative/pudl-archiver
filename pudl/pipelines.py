# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from . import items
import os


class PudlPipeline(object):

    def process_item(self, item, spider):
        """
        Process any item produced by the scrapers

        Args:
            item (scrapy.Item): the item we will process
            spider (scrapy.Spider): the spider that produced the item

        Returns:
            the item, altered if necessary
        """
        if isinstance(item, items.Eia860):
            return self.save_file(item)

        return item

    def save_file(self, item):
        """
        Save an Eia860 form

        Args:
            item (scrapy.Item): to be saved to hard drive, must have a
                save_path attribute

        Returns:
            unaltered item upon success
        """
        save_dir = os.path.dirname(item["save_path"])

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        with open(item["save_path"], "wb") as f:
            f.write(item["data"])

        return item

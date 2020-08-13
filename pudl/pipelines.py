# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from pathlib import Path


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
        # If we start collecting items that are not meant to be saved, change
        # write per-class processors
        return self.save_file(item)

    def save_file(self, item):
        """
        Save a single file

        Args:
            item (scrapy.Item): to be saved to hard drive, must have a
                save_path attribute

        Returns:
            unaltered item upon success
        """
        save_path = Path(item["save_path"])
        save_dir = save_path.parent

        if not save_dir.exists():
            save_dir.mkdir(parents=True)

        with save_path.open("wb") as f:
            f.write(item["data"])

        return item

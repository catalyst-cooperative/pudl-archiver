#!/usr/bin/env sh

for spider in $(scrapy list)
    do
    scrapy crawl $spider
    done

epacems.py --verbose

#!/usr/bin/env sh

for spider in $(scrapy list)
    do
    scrapy crawl $spider
    done

epacems --verbose
eia_bulk_elec
ferc_xbrl -l

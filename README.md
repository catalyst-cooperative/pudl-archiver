# PUDL Scrapers

# Installation

To make automation easy, it is recommended that you use
[vex](https://github.com/sashahart/vex) to manage your python environment.

From your virtual environment:

    pip install -r requirements.txt
    pip install ./ # OR pip install -e ./
    # Done

# Output location

Logs are collected:
`[your home]/Downloads/pudl/scrape/`

Data from the scrapers is stored:
`[your home]/Downloads/pudl/scrape/[source_name]/[today #]`


# Running the scrapers

The general pattern is `scrapy crawl [source_name]` for one of the supported
sources.  Typically and additional "year" argument is available, in the form
`scrapy crawl [source_name] -a year=[year]`.

See below for exact commands and available arguments.

## EPA CEMS

For full instructions:

`epacems.py --help`

## EIA860

To collect all the data:

`scrapy crawl eia860`

To collect a specific year (eg, 2007):

`scrapy crawl eia860 -a year=2007`


## EIA861

To collect all the data:

`scrapy crawl eia923`

To collect a specific year (eg, 2007):

`scrapy crawl eia861 -a year=2007`


## EIA923

To collect all the data:

`scrapy crawl eia923`

To collect a specific year (eg, 2007):

`scrapy crawl eia923 -a year=2007`


## FERC Form 1

To collect all the data:

`scrapy crawl ferc1`

To collect a specific year (eg, 2007):

`scrapy crawl ferc1 -a year=2007`


## IPM NEEDS6

To collect all the data:

`scrapy crawl ipm`

No additional arguments are supported.

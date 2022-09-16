# PUDL Scrapers

# Installation

We recommend using conda to create and manage your environment.

Run:
```
conda env create -f environment.yml
conda activate pudl-scrapers
```

# Output location

Logs are collected:
`[your home]/Downloads/pudl_scrapers/scraped/`

Data from the scrapers is stored:
`[your home]/Downloads/pudl_scrapers/scraped/[source_name]/[today #]`


# Running the scrapers

The general pattern is `scrapy crawl [source_name]` for one of the supported
sources.  Typically and additional "year" argument is available, in the form
`scrapy crawl [source_name] -a year=[year]`.

See below for exact commands and available arguments.

## 2010 Census DP1 GeoDatabase

`scrapy crawl censusdp1tract`

No other options.

## EPA CEMS

For full instructions:

`epacems --help`

## EIA Bulk Electricity Data

`eia_bulk_elec`

No other options.

## EPA CAMD to EIA Crosswalk

To collect the data and field descriptions:

 `scrapy crawl epacamd_eia`

## EIA860

To collect all the data:

`scrapy crawl eia860`

To collect a specific year (eg, 2007):

`scrapy crawl eia860 -a year=2007`


## EIA860M

To collect all the data:

`scrapy crawl eia860m`

To collect a specific month & year (eg, August 2020):

`scrapy crawl eia860 -a month=August -a year=2020`


## EIA861

To collect all the data:

`scrapy crawl eia861`

To collect a specific year (eg, 2007):

`scrapy crawl eia861 -a year=2007`


## EIA923

To collect all the data:

`scrapy crawl eia923`

To collect a specific year (eg, 2007):

`scrapy crawl eia923 -a year=2007`


## FERC Forms 1, 2, 6, & 60:

To collect all the data:

```sh
scrapy crawl ferc1
scrapy crawl ferc2
scrapy crawl ferc6
scrapy crawl ferc60
```

There are no subsets enabled.

## FERC 714
To collect the data:

`scrapy crawl ferc714`

There are no subsets, that's it.

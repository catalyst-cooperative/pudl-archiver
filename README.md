# PUDL Scrapers

# Installation

We recommend using conda to create and manage your environment.

Run:
```
conda env create -f environment.yml
conda activate pudl-scrapers
```

If you don't want to use conda, create a virtual environment and run:

```
pip install -r requirements.txt
pip install ./ # OR pip install -e ./
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

## 2010 Census GeoData

`scrapy crawl censusdp1tract`

No other options.

## EPA CEMS

For full instructions:

`epacems --help`

## EPA-EIA Crosswalk

 To collect the data and field descriptions:

 `scrapy crawl epa_eia_crosswalk`

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


## FERC Form 1

To collect all the data:

`scrapy crawl ferc1`

To collect a specific year (eg, 2007):

`scrapy crawl ferc1 -a year=2007`

## FERC 714
To collect the data:

`scrapy crawl ferc714`

There are no subsets, that's it.

## EIP Infrastructure
To collect the data:

`scrapy crawl eipinfrastructure`

There are no subsets, that's it.

## LBLN ISO Queues
To collect the data:

`scrapy crawl lbnlisoqueues`

There are no subsets, that's it.


## IPM NEEDS6

To collect all the data:

`scrapy crawl epaipm`

No additional arguments are supported.

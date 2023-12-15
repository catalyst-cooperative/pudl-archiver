# PUDL Archivers

This repo implements data archivers for The Public Utility Data Liberation Project
([PUDL](https://github.com/catalyst-cooperative/pudl)). It is responsible for downloading
raw data from multiple sources, and create Zenodo archives containing that data.

## Background on Zenodo

[Zenodo](https://zenodo.org/) is an open repository maintained by CERN that allows users
to archive research-related digital artifacts for free. Catalyst uses Zenodo to archive
raw datasets scraped from the likes of FERC, EIA, and the EPA to ensure reliable,
versioned access to the data PUDL depends on. Take a look at our archives
[here](https://zenodo.org/communities/catalyst-cooperative/?page=1&size=20). In the
event that any of the publishers change the format or contents of their data, remove old
years, or simply cease to exist, we will have a permanent record of the data. All data
uploaded to Zenodo is assigned a DOI for streamlined access and citing.

Whenever the historical data changes substantially or new years are added, we make new
Zenodo archives and build out new versions of PUDL that are compatible. Paring specific
Zenodo archives with PUDL releases ensures a functioning ETL for users and developers.

Once created, Zenodo archives cannot be deleted. This is, in fact, their purpose! It
also means that one ought to be sparing with the information uploaded. We don't want
wade through tons of test uploads when looking for the most recent version of data.
Luckily Zenodo has created a sandbox environment for testing API integration. Unlike the
regular environment, the sandbox can be wiped clean at any time. When testing uploads,
you'll want to upload to the sandbox first. Because we want to keep our Zenodo as clean
as possible, we keep the upload tokens internal to Catalyst. If there's data you want to
see integrated, and you're not part of the team, send us an email at
<hello@catalyst.coop>.

One last thing-- Zenodo archives for particular datasets are referred to as
"depositions". Each dataset is it's own deposition that gets created when the dataset is
first uploaded to Zenodo and versioned as the source releases new data that gets
uploaded to Zenodo.

## Installation

We recommend using conda to create and manage your environment.

Run:

```
conda env create -f environment.yml
conda activate pudl-cataloger
```

## Setting up environment

API tokens are required to interact with Zenodo. There is one set of tokens for accessing
the sandbox server, and one for the production server. The archiver tool expects these tokens
to be set in the following environment variables: `ZENODO_TOKEN_PUBLISH` and `ZENODO_TOKEN_UPLOAD`
or `ZENODO_SANDBOX_TOKEN_PUBLISH` and `ZENODO_SANDBOX_TOKEN_UPLOAD` for the sandbox server.
Catalyst uses a set of institutional tokens - you can contact a maintainer for tokens.

If you want to interact with the `epacems` archiver, you'll need to get a
[personal API](https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup) key and
store it as an environment variable at `EPACEMS_API_KEY`.

## Usage

A CLI is provided for creating and updating archives. The basic usage looks like:

```
pudl_archiver --datasets {list_of_datasources}
```

This command will download the latest available data and create archives for each
requested datasource requested. The supported datasources include `eia860`, `eia861`, `eia923`,
`eia_bulk_elec`, `epacems`, `epacamd_eia`, `ferc1`, `ferc2`, `ferc6`, `ferc60`,
`ferc714`, `eia860m`, `mshamines`.

There are also four optional flags available:

- `--sandbox`: used for testing. It will only interact with Zenodo's
  [sandbox](https://sandbox.zenodo.org/) instance.
- `--initialize`: used for creating an archive for a new dataset that doesn't
  currently exist on zenodo. If successful, this command will automatically add
  the new Zenodo DOI to the `dataset_doi.yaml` file.
- `--dry-run`: used for testing, it ignores all Zenodo write operations.
- `--all`: shortcut for archiving all datasets that we have defined archivers
  for. Overrides `--datasets`.

## Adding a new dataset

### Step 1: Implement archiver interface

All of the archivers inherit from the `AbstractDatasetArchiver` base class (defined
in `src/pudl_archiver/archiver/classes.py`. There is only a single method that each
archiver needs to implement. That is the `get_resources` method. This method will be
called by the base class to coordinate downloading all data-resources. It should be
a generator that yields awaitables to download those resources. Those awaitables
should be coroutines that download a single resource, and return a path to that
resource on disk, and a dictionary of working partitions relevant to the resource.
In practice this generally looks something like:

```py
class ExampleArchiver(AbstractDatasetArchiver):
    name = "example"

    async def get_resources(self) -> ArchiveAwaitable:
        for year in range(start_year, end_year):
            yield self.download_year(year)

    async def download_year(self, year: int) -> tuple[Path, dict]:
        url = f"https://example.com/example_form_{year}.zip"
        download_path = self.download_directory / f"example_form_{year}.zip"
        await self.download_zipfile(url, download_path)

        return download_path, {"year": year}
```

This example uses a couple of useful helper methods/variables defined in the base
class. Notice, `download_year` uses `self.download_directory` this is a temporary
directory created and manged by the base class that is used as a staging area for
downloading data before uploading it to Zenodo. This temporary directory will be
automatically removed once the data has been uploaded. `download_year` also uses the
method `download_zipfile`. This is a helper method implemented to handle downloading
zipfiles that includes a check for valid zipfiles, and a configurable number of
retries. Not shown here, but also used frequently is the `get_hyperlinks` method.
This helper method takes a URL, and a `regex` pattern, and it will find all
hyperlinks matching the pattern on the page pointed to by the URL. This is useful if
there's a page containing links to a series of data resources that have somewhat
structured names.

### Step 2: Run --initialize command

You will need to run the initialize command to create a new zenodo deposition, and
update the config file with the new DOI:

```
pudl_archiver --datasets {new_dataset_name} --initialize
```

## Development

We only have one development specific tool, which is the Zenodo Postman collection in `/devtools`.
This tool is used for testing and prototyping Zenodo API calls, it is not needed to use the archiver
tool itself.

To use it:

1. download [Postman](https://www.postman.com/) (or use their web client)
2. import this collection
3. set up a `publish_token` Postman environment variable like in the [docs](https://learning.postman.com/docs/sending-requests/variables/#variable-scopes)
4. send stuff to Zenodo by clicking buttons in Postman!

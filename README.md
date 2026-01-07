# PUDL Archivers

This repo implements data archivers for The Public Utility Data Liberation Project
([PUDL](https://github.com/catalyst-cooperative/pudl)). It is responsible for downloading
raw data from multiple sources, and create Zenodo archives containing that data.

## Background on Zenodo

[Zenodo](https://zenodo.org/) is an open repository maintained by CERN that allows users
to archive research-related digital artifacts for free. Catalyst uses Zenodo to archive
raw datasets scraped from the likes of FERC, EIA, and the EPA to ensure reliable,
versioned access to the data PUDL depends on. Take a look at our archives
[here](https://zenodo.org/communities/catalyst-cooperative/). In the
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

We use `pixi` to create and manage the `pudl-archiver` development environment.

First, follow the [installation instructions](https://pixi.sh/latest/#installation) for your operating system.

Next, run:

```bash
pixi run pre-commit-install
pixi shell
```

This will setup and activate the environment, and install the pre-commit hooks.

> [!TIP]
> Instead of `pixi shell`, you can also use `pixi run` to run the archiver code in
> the correct Python environment (e.g., `pixi run pudl_archiver --dataset eiawater`).
>
> If you are running integration tests locally, you'll need to use the "tests" pixi
> environment (e.g., `pixi shell -e tests`; `pixi run -e tests pytest ...`)

## Setting up the development environment

API tokens are required to interact with Zenodo. There is one set of tokens for accessing
the sandbox server, and one for the production server. The archiver tool expects these tokens
to be set in the following environment variables: `ZENODO_TOKEN_PUBLISH` and `ZENODO_TOKEN_UPLOAD`
or `ZENODO_SANDBOX_TOKEN_PUBLISH` and `ZENODO_SANDBOX_TOKEN_UPLOAD` for the sandbox server.
Catalyst uses a set of institutional tokens - you can contact a maintainer for tokens.

If you want to interact with the `epacems` archiver, you'll need to get a
[personal API](https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup) key and
store it as an environment variable at `EPACEMS_API_KEY`.

## Usage

A CLI is provided for creating and updating archives. This CLI provides several
commands for archiving data to different storage backends. For a high-level overview
of these commands, run:

```bash
pudl_archiver --help
```

### Datasets
Archivers have been developed for a number of datasets, including `eia860`, `eia923`,
`ferc1`, `epacems`, and many more; see the full list of available datasets with
`pudl_archiver list-datasets`. Any dataset can be used with any of the supported
storage backends by specifying the `depositor`, which will be explained in the
following section.

### Depositors
For each supported storage backend, there is a ``depositor`` that provides an interface
for creating archives using that backend. The basic command to run the archiver with
a using a given depositor looks like the following:

```bash
pudl_archiver archive {depositor} {dataset}
```

Different backends have different arguments and options that can or must be passed to
the CLI, so refer to the section below corresponding to a given backend before trying
to use this command. There are also several options that apply to all backends, including:

- `--initialize`: used for creating an archive for a new dataset that doesn't
  currently exist on zenodo. If used with the `fsspec` backend, this will attempt
  to create a new directory at the location specified by `deposition-path` and use
  this directory for archiving.
- `--auto-publish`: Automatically publish new version of an archive if all validation

For the full list of options, run:

```bash
pudl_archiver archive {depositor} --help
```

#### Zenodo
The Zenodo backend is used for most of our production archives. The only option
specific to Zenodo is `--sandbox`, which can be used to toggle the usage of the
Zenodo sandbox server for testing.

### fsspec
The [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) depositor can
be used with any `fsspec` compatible filesystem. This functionality is necessary
for datasets like FERC EQR, which require significantly more storage space than is
available in a standard Zenodo archive, but also can be useful for testing. For
example, if you are developing a new archiver or updating an existing one, you
can use the `fsspec` depositor with a local path to download data directly to
your computer for inspection. The `fsspec` depositor requires a `deposition-path`
argument to specify the base path where archives should be created. Basic usage
looks like the following:

```bash
pudl_archiver archive fsspec {dataset} {deposition-path}
```

The `deposition-path` argument should be formatted like `protocol://path/to/deposition`.
For a local path, you would use the protocol `file`, while `gs` can be used to specify
a GCS bucket.

### Retrying a failed run
All runs for the archiver will output a Run Summary file in the current working directory
called `{dataset}_run_summary.json`, which contains information about the run, including
any errors that were encountered during the run. The `retry-run` command can be used to
retry such a run which encountered errors without having to re-download files that were
successfully added to the archive in the previous run. Basic usage looks like the
following:

```bash
pudl_archiver retry-run {run_summary_json_file}
```

This command will inherit all settings from the previous run except `--auto-publish` to
avoid accidental publication. This also allows `retry-run` to be used to publish results
from a successful run which did not have `--auto-publish` set.

During a retry, the archiver expects all successfully downloaded resources to still
be in the draft deposition. If the state of the deposition has been changed in any
way since the failed run, then a retry may produce unexpected results.

Once a retry run has been kicked off, it will follow these steps below:

1. Load the run summary from the previous run and get failed/successful resources
and CLI settings.
2. Filter out successful resources from previous run so we don't re-download them.
3. Start downloading failed resources and adding new versions to the open draft deposition.
4. Attempt to publish draft containing resources from original run and retry, following
standard validation procedures.

## Adding a new dataset

### Step 1: Define the dataset's metadata

> [!IMPORTANT]
> Throughout the code, the dataset you choose will be referred to by a shorthand code -
> e.g.,`eia860` or `mshamines` or `nrelatb`. The standard format we use for naming
> datasets is `agency name` + `dataset name`. E.g., Form 860 from EIA becomes `eia860`.
> When the name of the dataset is more ambiguous
> (e.g., [MSHA's mine datasets](https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp)),
> we aim to choose a name that is as indicative as possible - in this case, `mshamines`.
> If you're unsure which name to choose, ask early in the contribution process as this
> will get encoded in many locations.

For each dataset we archive, we record information about the title, a description, who
contributed to archiving the dataset, the segments into which the data files are
partitioned, its license and keywords. This information is used to communicate about
the dataset's usage and provenance to any future users.

- Title: The title of your dataset should clearly contain the agency publishing the data
and a non-abbreviated title (e.g., EIA Manufacturing Energy Consumption Survey, not EIA
MECS).
- Path: The link to the dataset's "homepage", where information about the dataset and
the path to download it can be found.
- Working partitions: A dictionary where the key is the name of the partition (e.g.,
month, year, form), and the values are the actual available partitions (e.g.,
2002-2020).
- License: We only archive data with an open source license (e.g., US Government Works
or a Creative Commons License), so make sure any data you're archiving is licensed for
re-distribution.
- Keywords: Words that someone might use to search for this dataset. These are used to
help people find our data on Zenodo.

If your dataset will be integrated directly into
[PUDL](https://github.com/catalyst-cooperative/pudl), you'll need to add the metadata
for the dataset into the PUDL repository in the `SOURCES` dictionary in
`src.pudl.metadata.sources.py`.

If you aren't sure, or you're archiving data that won't go into PUDL, you'll want to
add your metadata as an entry into the `NON_PUDL_SOURCES` dictionary in
`src/pudl_archiver/metadata/sources.py`.

### Step 2: Implement archiver interface

All of the archivers inherit from the `AbstractDatasetArchiver` base class (defined
in `src/pudl_archiver/archiver/classes.py`), which coordinates the process of downloading,
uploading and validating archives.

There is only a single method that each archiver needs to implement. That is the
`get_resources` method. This method will be called by the base class to coordinate
downloading all data-resources. It should be a generator that yields awaitables to
download those resources. Those awaitables should be coroutines that download a
single resource. They should return a path to that resource on disk, and a
dictionary of working partitions relevant to the resource. In practice this generally
looks something like:

```py
BASE_URL = "https://www.eia.gov/electricity/data/eia860"

class Eia860Archiver(AbstractDatasetArchiver):
    name = "eia860"

    async def get_resources(self) -> ArchiveAwaitable:
        """Download EIA-860 resources."""
        link_pattern = re.compile(r"eia860(\d{4})(ER)*.zip")
        for link in await self.get_hyperlinks(BASE_URL, link_pattern):
            matches = link_pattern.search(link)
            if not matches:
                continue
            year = int(matches.group(1))
            if self.valid_year(year):
                yield self.get_year_resource(link, year)

    async def get_year_resource(self, link: str, year: int) -> ResourceInfo:
        """Download zip file."""
        # Append hyperlink to base URL to get URL of file
        url = f"{BASE_URL}/{link}"
        download_path = self.download_directory / f"eia860-{year}.zip"
        await self.download_zipfile(url, download_path)

        return ResourceInfo(local_path=download_path, partitions={"year": year})
```

#### Create a new archiver script

1. To create a new archiver, create a new Python file in `src.pudl_archiver.archivers`.
Files for archivers produced by the same agency are sub-categorized into folders (e.g.,
`src.pudl_archiver.archivers.eia`).
2. Subclass the `AbstractDatasetArchiver` to create an archiver class for your dataset -
e.g., `NrelAtbArchiver` or `PhmsaGasArchiver`.
3. Define the `name` of your dataset to be the shorthand code you defined in Step 1 (e.g.,
`eia860`). This should match the name you used for the dictionary key in the metadata
sources dictionary.

#### Defining `get_resources`

`get_resources()` is the core method required for every archiver - it should identify
every link or API call needed to download all the data, and yield a series of
awaitables that will download each partition of the data. These partitions should match
the partitions you defined in step 1 (e.g., one file per year). The content of this
method will vary depending on the format and accessibility of the dataset that you are
archiving, but typically tends to follow one of the following patterns:

- Yields an awaitable downloading a single known link (see `archivers.census.censusdp1tract.py`)
- Gets all of the links on a page, identifies relevant links using a regex pattern, and
yields awaitables downloading each link on the page (see `archivers.eia.eia860.py` or
`archivers.eia.eiamecs.py`). This relies on the frequently used `get_hyperlinks` method.
This helper method takes a URL, and a `regex` pattern, and it will find all hyperlinks
matching the pattern on the page pointed to by the URL. This is useful if there's a page
containing links to a series of data resources that have somewhat structured names.
- Calls an API to identify download queries for each partition of the data, and yields
awaitables downloading each partition of the data from the API (see
`archivers.eia.epacems.py`).

In the example above, `get_resources` is defined as follows:

```py
async def get_resources(self) -> ArchiveAwaitable:
  """Download EIA-860 resources."""
  link_pattern = re.compile(r"eia860(\d{4})(ER)*.zip")
  for link in await self.get_hyperlinks(BASE_URL, link_pattern):
      matches = link_pattern.search(link)
      if not matches:
          continue
      year = int(matches.group(1))
      if self.valid_year(year):
          yield self.get_year_resource(link, year)
```

In this case, we know that Form 860 data is on a webpage (`BASE_URL`) containing a
series of download links, and that the links to the data we want follow a general pattern:
they are called `eia860{year}.zip` or `eia860{year}ER.zip`. We search through all the
links in `BASEURL` to find links that match this pattern. For each matching link, we
extract the year from the file name and pass both the link and the year to the
`get_year_resource()` method.

> [!TIP]
> `self.valid_year()` is an optional method that allows us to easily run the archiver on
> only a year or two of data, for datasets partitioned by year. Though optional, it helps
> to speed up testing of the data. The method expects a year and returns a boolean
> indicating whether or not the year is valid.

#### Getting each individual resource

In the example above, we define a second async method. This method downloads a single
file per partition:

```py
async def get_year_resource(self, link: str, year: int) -> ResourceInfo:
  """Download zip file."""
  # Append hyperlink to base URL to get URL of file
  url = f"{BASE_URL}/{link}"
  download_path = self.download_directory / f"eia860-{year}.zip"
  await self.download_zipfile(url, download_path)

  return ResourceInfo(local_path=download_path, partitions={"year": year})
```

This method should handle the following steps:

- identify the specific download link for the file(s) in the partition
- rename the file to match our data conventions. We rename files to match the format
`datasource-partition.ext` - e.g. `eia860-1990.zip`.
- construct the path to where we want to temporarily store the file locally, using
`self.download_directory`: this is a temporary directory created and manged by the base
class that is used as a staging area for downloading data before uploading it to its
final location (e.g. Zenodo, a cloud bucket).
This temporary directory will be automatically removed once the data has been uploaded.
- return `ResourceInfo`, where `local_path` is the path to the file's location in
`self.download_directory` and `partitions` is a dictionary specifying the partition(s)
of the dataset. We'll use this to coordinate validation and upload once all files have
been downloaded.

We have written a number of download methods to handle different file formats:

- You're downloading a zipfile: `self.download_zipfile()` is a helper method implemented
to handle downloading zipfiles that includes a check for valid zipfiles, and a
configurable number of retries.
- You're downloading a single file in another format (e.g., Excel):
`self.download_and_zipfile()` downloads a file and zips it. Where the original files are
not already zipped, we zip them to speed up upload and download times. See
`archivers.censuspep.py` for an example of this method.
- You're downloading a number of files that belong to a single partition (e.g., multiple
API calls per year): `self.add_to_archive()` can be used to
download multiple files and add them to the same zipfile. See `archivers.eia.eia860m.py`
for an example of this method.

### Step 3: Test archiver locally

Once you've written your archiver, it's time to test that it works as expected! To run
the archiver locally, run the following commands in your terminal:

```bash
pudl_archiver --datasets {new_dataset_name} --initialize --summary-file {new_dataset_name}-summary.json --depositor fsspec --deposition-path {file://local/path/to/folder}
```

- `--initialize` creates a new deposition, and is used when creating a brand new archive
- `--summary-file` will save a .json file summarizing the results of all
validation tests, which is useful for reviewing your dataset.
- `--depositor` selects the backend engine used for archive storage - in this case,
we save files locally, but by default this uploads files to Zenodo.
- `--depositor-path`: the path to the folder where you want to download local files for
inspection.

Run the archiver and review the output in the specified folder, iterating as needed to
ensure that all files download as expected.

### Step 4: Test uploading to Zenodo

Once you're satisfied with your archiver, it's time to upload it to the Zenodo sandbox
so that others can review it. The [Zenodo sandbox](https://sandbox.zenodo.org/) allows
you to create temporary Zenodo archives before publishing your data to the production
server. We use the sandbox to test our archives, review each other's work, and attempt
data integration into PUDL prior to publishing our archives on the main Zenodo site.

Note that this step will require you to create your own
[Zenodo sandbox credentials](https://zenodo.org/account/settings/applications/tokens/new/)
if you are not a core Catalyst developer. Each token should have the following permissions:

- `ZENODO_SANDBOX_TOKEN_UPLOAD`: deposit:write, user:email
- `ZENODO_SANDBOX_TOKEN_PUBLISH`: deposit:actions, deposit:write, user:email

Once created, you'll need to save each token as follows:

```bash
echo "export ZENODO_SANDBOX_TOKEN_UPLOAD='token'" >> ~/.zshrc # if you are using zsh
echo "export ZENODO_SANDBOX_TOKEN_UPLOAD='token'" >> ~/.bashrc # if you are using bash
set -Ux "export ZENODO_SANDBOX_TOKEN_UPLOAD='token' # if you are using fish shell
mamba reactivate pudl-cataloger
```

Like before, you will need to run the initialize command to create a new Zenodo deposition:

```bash
pudl_archiver --datasets {new_dataset_name} --initialize --sandbox --summary-file {new_dataset_name}-summary.json
```

### Step 5: Manually review your archive before publication

If the archiver run is successful, it will produce a link to the draft sandbox archive. Though
many of the validation steps are automated, it is worthwhile manually reviewing archives
before publication, since a Zenodo record cannot be deleted once published. Here are
some recommended additional manual steps for verification:

1. Open the `*-summary.json` file that your archiver run produced. It will contain
the `name`, `description` and `success` of each test run on the archive, along with any
notes. If your draft archive was successfully created all tests have passed, but it's
worthwhile skimming through the file to make sure all expected tests have been run and
there are no notable warnings in the `notes`.
2. On Zenodo, "preview" the draft archive and check to see that nothing seems unusual
(e.g., missing years of data, new partition formats, contributors).
3. Look at the `datapackage.json`. Does the dataset metadata look as expected? How about
the metadata for each resource?
4. Click to download one or two files from the archive. Extract them and open them to
make sure they look as expected.

When you're ready to submit this archive, hit "publish"! Add this sandbox archive link
to your pull request and request a review from a Catalyst core member.

*If your dataset is destined for integration into PUDL:* Head over to the
[pudl](https://github.com/catalyst-cooperative/pudl) repo to attempt to integrate the
new archive using the sandbox DOI. This will help to flag any formatting problems
before publishing to the production server.

### Step 6: Finalizing the archive

> [!IMPORTANT]
> This step can only be done by core Catalyst developers, as it requires credentials
> to our production Zenodo account. We'll handle this step as part of the PR review
> process.

Once your PR has been approved, it's time for your archive to make its debut!

- Rerun the archiver without the `--sandbox` flag to create a draft production archive
- Review the archive using the guidelines in Step 5.
- Once published, submit the archive to the Catalyst Cooperative community.
- Add the **concept DOIs** for the published sandbox and production to
`/src/pudl_archiver/package_data/zenodo_doi.yaml`. These DOIs tell the archiver when a
dataset already exists, making it possible to update existing archives with new data.
This is the DOI listed after "Cite all versions? You can cite all versions by using the DOI"
on the Zenodo webpage under the list of versions published. For instance, our [archive of EIA Form 860
](https://doi.org/10.5281/zenodo.4127026) has the concept DOI 10.5281/zenodo.4127026.
- If you implemented `self.valid_year()`, add your dataset manually to the list of datasets
that support this feature in `src/pudl_archiver/cli.py` under the `--only-years` flag
description.

### Step 7: Automate archiving

We automatically run all our archivers once a month to make sure we capture ongoing
changes to our archived datasets. To automate archiving of your new dataset, add the
dataset to the list of quoted datasets in `.github/workflows/run-archiver.yml` where the
`default` value of `datasets` is configured (line 9), as well as where the `dataset`
inputs for the `matrix` are set (line 28). Also add the dataset to
the list of datasets in `.github/ISSUE_TEMPLATE/monthly-archive-update.md` to ensure
that we track validation failures and publish the latest release with our automated
monthly archive run.

## Development

### Updating the development environment

To add a package to your pixi environment, simply run:

```bash
pixi add {package}
pixi add --pypi {package} # To specify the package should be installed using pypi
```

Then, update your pixi.lock file using:
```bash
pixi update
```

See the [Pixi documentation](https://pixi.sh/v0.24.2/) for more information.

### Using postman

We only have one development specific tool, which is the Zenodo Postman collection in `/devtools`.
This tool is used for testing and prototyping Zenodo API calls, it is not needed to use the archiver
tool itself.

To use it:

1. download [Postman](https://www.postman.com/) (or use their web client)
2. import this collection
3. set up a `publish_token` Postman environment variable like in the
[docs](https://learning.postman.com/docs/sending-requests/variables/#variable-scopes)
4. send stuff to Zenodo by clicking buttons in Postman!

## Supporting PUDL

These raw data archivers are part of the Public Utility Data Liberation Project (PUDL),
a project of [Catalyst Cooperative](https://catalyst.coop). PUDL has been made possible
by the generous support of our sustainers, grant funders, and volunteer open source
contributors.

If you would like to support the ongoing development of PUDL, please consider
[becoming a sustainer](https://opencollective.com/pudl).

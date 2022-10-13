#!/usr/bin/env python
"""Setup script to install the PUDL scrapers repo as a package."""
from setuptools import find_packages, setup

setup(
    name="pudl_scrapers",
    version="0.2.0",
    author="PUDL",
    python_requires=">=3.10,<3.11",
    install_requires=[
        "catalystcoop.pudl @ git+https://github.com/catalyst-cooperative/pudl.git@dev",
        "coloredlogs~=15.0",
        "factory_boy>=2.12",
        "feedparser>=6.0",
        "tqdm>=4.64",
        "catalystcoop.arelle-mirror==1.3.0",
        "python-dotenv~=0.21.0",
        "semantic_version>=2.8,<3",
    ],
    extras_require={
        "dev": [
            "black>=22.0,<22.9",  # A deterministic code formatter
            "isort>=5.0,<5.11",  # Standardized import sorting
            "tox>=3.20,<3.27",  # Python test environment manager
            "twine>=3.3,<4.1",  # Used to make releases to PyPI
        ],
        "docs": [
            "doc8>=0.9,<1.1",  # Ensures clean documentation formatting
            "furo>=2022.4.7",
            "sphinx>=4,!=5.1.0,<5.1.2",  # The default Python documentation engine
            "sphinx-autoapi>=1.8,<1.10",  # Generates documentation from docstrings
            "sphinx-issues>=1.2,<3.1",  # Allows references to GitHub issues
        ],
        "tests": [
            "bandit[toml]>=1.6,<1.8",  # Checks code for security issues
            "coverage>=5.3,<6.5",  # Lets us track what code is being tested
            "doc8>=0.9,<1.1",  # Ensures clean documentation formatting
            "flake8>=4.0,<5.1",  # A framework for linting & static analysis
            "flake8-builtins>=1.5,<1.6",  # Avoid shadowing Python built-in names
            "flake8-colors>=0.1,<0.2",  # Produce colorful error / warning output
            "flake8-docstrings>=1.5,<1.7",  # Ensure docstrings are formatted well
            "flake8-rst-docstrings>=0.2,<0.3",  # Allow use of ReST in docstrings
            "flake8-use-fstring>=1.0,<1.5",  # Highlight use of old-style string formatting
            "mccabe>=0.6,<0.8",  # Checks that code isn't overly complicated
            "pep8-naming>=0.12,<0.14",  # Require PEP8 compliant variable names
            "pre-commit>=2.9,<2.21",  # Allow us to run pre-commit hooks in testing
            "pydocstyle>=5.1,<6.2",  # Style guidelines for Python documentation
            "pytest>=6.2,<7.2",  # Our testing framework
            "pytest-console-scripts>=1.1,<1.4",  # Allow automatic testing of scripts
            "pytest-cov>=2.10,<3.1",  # Pytest plugin for working with coverage
            "pytest-mock>=3.0,<3.9",  # Pytest plugin for mocking function calls and objects
            "rstcheck[sphinx]>=5.0,<6.2",  # ReStructuredText linter
        ],
    },
    # A controlled vocabulary of tags used by the Python Package Index.
    # Make sure the license and python versions are consistent with other arguments.
    # The full list of recognized classifiers is here: https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    # Directory to search recursively for __init__.py files defining Python packages
    packages=find_packages("src"),
    # Location of the "root" package:
    package_dir={"": "src"},
    # package_data is data that is deployed within the python package on the
    # user's system. setuptools will get whatever is listed in MANIFEST.in
    include_package_data=True,
    # entry_points defines interfaces to command line scripts we distribute.
    # Can also be used for other resource deployments, like intake catalogs.
    entry_points={
        "console_scripts": [
            "pudl_archiver=pudl_scrapers.cli:main",
        ]
    },
)

#!/usr/bin/env python
"""Setup script to install the PUDL scrapers repo as a package."""
from setuptools import find_packages, setup

setup(
    name="pudl_scrapers",
    version="0.2.0",
    author="PUDL",
    # Directory to search recursively for __init__.py files defining Python packages
    packages=find_packages("src"),
    # Location of the "root" package:
    package_dir={"": "src"},
    python_requires=">=3.10,<3.11",
    install_requires=[
        "factory_boy>=2.12",
        "feedparser>=6.0",
        "scrapy>=1.7",
        "tqdm>=4.64",
        "catalystcoop.arelle-mirror==1.3.0",
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
            "sphinx>=4,!=5.1.0,<5.2.3",  # The default Python documentation engine
            "sphinx-autoapi>=1.8,<2.1",  # Generates documentation from docstrings
            "sphinx-issues>=1.2,<3.1",  # Allows references to GitHub issues
        ],
        "tests": [
            "bandit[toml]>=1.6,<1.8",  # Checks code for security issues
            "coverage>=5.3,<6.6",  # Lets us track what code is being tested
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
            "pytest-cov>=2.10,<4.1",  # Pytest plugin for working with coverage
            "pytest-mock>=3.0,<3.9",  # Pytest plugin for mocking function calls and objects
            "rstcheck[sphinx]>=5.0,<6.2",  # ReStructuredText linter
        ],
    },
    entry_points={
        "console_scripts": [
            "epacems=pudl_scrapers.bin.epacems:main",
            "eia_bulk_elec=pudl_scrapers.bin.eia_bulk_elec:main",
            "ferc_xbrl=pudl_scrapers.bin.ferc_xbrl:main",
        ]
    },
)

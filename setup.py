#!/usr/bin/env python
"""Setup script to install the PUDL scrapers repo as a package."""
from setuptools import find_packages, setup

setup(
    name="pudl_scrapers",
    version="0.2.0",
    author="PUDL",
    packages=find_packages(),
    python_requires=">=3.10,<3.11",
    install_requires=[
        "factory_boy>=2.12",
        "scrapy>=1.7",
        "pytest>=5.2",
        "tqdm>=4.64",
    ],
    entry_points={
        "console_scripts": [
            "epacems=pudl_scrapers.bin.epacems:main",
            "eia_bulk_elec=pudl_scrapers.bin.eia_bulk_elec:main",
        ]
    },
)

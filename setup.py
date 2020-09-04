#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

setup(name="PudleScrapers",
      version="0.2.0",
      author="PUDL",
      packages=find_packages(),
      entry_points={"console_scripts": ["epacems=pudl_scrapers.bin.epacems:main"]})

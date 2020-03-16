#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

setup(name="PudleScrapers",
      version="0.2.0",
      author="PUDL",
      packages=find_packages(),
      scripts=["pudl/bin/epacems.py"])

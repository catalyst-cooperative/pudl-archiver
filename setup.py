#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

setup(name="PudleScrapers",
      version="0.1.1",
      author="PUDL",
      packages=find_packages(),
      scripts=["pudl/bin/epacems.py"])

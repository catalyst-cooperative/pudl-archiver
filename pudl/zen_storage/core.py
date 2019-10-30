# -*- coding: utf-8 -*-

import json
import logging
import requests


class ZenStorage:
    """Thin interface to store data with zenodo.org via their api"""

    def __init__(self, key, logger=None, testing=False):
        """
        Prepare the ZenStorage interface

        Args:
            key (str): The API key required to authenticate with Zenodo
            testing (bool): If true, use the Zenodo sandbox api rather than the
                production service

        Returns:
            ZenStorage
        """
        if logger is None:
            logger = logging.Logger(__name__)

        self.logger = logger
        self.key = key

        if testing:
            self.api_root = "https://sandbox.zenodo.org/api"
        else:
            self.api_root = "https://zenodo.org/api"

    def get_deposition(self, title):
        """
        Get the Deposition (see developers.zenodo.org) id for a given form
            title
        Args:
            title (str): the name of the PUDL deposition resource, such as
                Eia860

        Returns:
            deposition data as dict, per
            https://developers.zenodo.org/?python#depositions
        """
        url = self.api_root + "/deposit/depositions"
        params = {"q": 'title:"%s"' % title, "access_token": self.key}

        lookup = requests.get(url, params)

        jsr = lookup.json()

        if lookup.status_code != 200:
            msg = "Could not look up deposition: %s" % jsr
            self.logger.error(msg)
            raise RuntimeError(msg)

        if len(jsr) != 1:
            self.logger.warning("Expected single deposition, got %d" %
                                len(jsr))
            return

        return jsr[0]

    def create_deposition(self, metadata):
        """
        Create a deposition resource.  This should only be called once for a
        given data source.  The deposition will be prepared in draft form, so
        that files can be added prior to publication.

        Args:
            metadata: deposition metadata as a dict, per
            https://developers.zenodo.org/?python#representation

        Returns:
            deposition data as dict, per
            https://developers.zenodo.org/?python#depositions
        """
        url = self.api_root + "/deposit/depositions"
        params = {"access_token": self.key}
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"metadata": metadata})

        response = requests.post(
            url, params=params, data=data, headers=headers)
        jsr = response.json()

        if response.status_code != 201:
            msg = "Could not create deposition: %s" % jsr
            self.logger.error(msg)
            raise RuntimeError(msg)

        return jsr

    def new_eia860_version(self):
        """
        Produce a new version of the Eia860 Zenodo archive, whether or not one
        already exists

        Returns:
            deposition data as dict, per
            https://developers.zenodo.org/?python#depositions
        """
        deposition = self.get_deposition("Eia860 Source")

        if deposition is None:
            metadata = {
                "title": "Eia860 Source",
                "upload_type": "dataset",
                "description": "Eia860 Form Data, archived from "
                               "https://www.eia.gov/electricity/data/eia860/",
                "creators": [
                    {"name": "Source: US Energy Information Administration"},
                    {"name": "Uploaded: Catalyst Cooperative"}
                ],
                "access_right": "open",
                "keywords": ["eia860", "electricity", "usa"]
            }

            return self.create_deposition(metadata)

        if deposition["state"] == "unsubmitted":
            return deposition

        url = self.api_root + "/deposit/depositions/%d/actions/newversion" \
            % deposition["id"]

        params = {"access_token": self.key}
        response = requests.post(url, params=params)
        jsr = response.json()

        if response.status_code != 201:
            msg = "Could not create new version: %s" % jsr
            self.logger.error(msg)
            raise RuntimeError(msg)

        return jsr

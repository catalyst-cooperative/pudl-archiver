# -*- coding: utf-8 -*-

import copy
import io
import os
import random
import requests

from pudl.zen_storage import ZenStorage


class TestZenStorage:
    """
    Ensure that we are able to use the Zenodo service, via sandbox. Integration
    test, accesses extertal resources.
    """

    zs = ZenStorage(key=os.environ["ZENODO_TEST_KEY"], testing=True)
    test_deposition = {
        "title": "PUDL Test",
        "upload_type": "dataset",
        "description": "Test dataset for the sandbox.  Thanks!",
        "creators": [{"name": "Catalyst Cooperative"}],
        "access_right": "open",
        "keywords": ["catalyst", "test", "cooperative"]
    }

    def test_lookup_and_create(self):
        """Ensure lookup and create processes work."""
        td = copy.deepcopy(self.test_deposition)
        td["title"] += ": %d" % random.randint(1000, 9999)

        lookup = self.zs.get_deposition(td["title"])
        assert(lookup is None)

        create = self.zs.create_deposition(td)

        for key, _ in td.items():
            assert(create["metadata"][key] == td[key])

        requests.post(
            create["links"]["publish"], data={"access_token": self.zs.key})

        lookup = self.zs.get_deposition(td["title"])
        assert(lookup["metadata"]["title"] == td["title"])

    def test_new_version_and_file_api_upload(self):
        """Ensure we can create new versions of a deposition"""
        # It would be better if we could test a single function at a time,
        # however the api does not support versioning without a file upload.

        td = copy.deepcopy(self.test_deposition)
        td["title"] += ": %d" % random.randint(1000, 9999)

        first = self.zs.create_deposition(td)
        fake_file = io.BytesIO(b"This is a test.")
        self.zs.file_api_upload(first, "test1.txt", fake_file)

        response = requests.post(first["links"]["publish"],
                                 data={"access_token": self.zs.key})
        published = response.json()

        if response.status_code > 299:
            raise AssertionError(
                "Failed to save test deposition: code %d, %s" %
                (response.status_code, published))

        assert published["state"] == "done"
        assert published["submitted"]

        new_version = self.zs.new_deposition_version(td)

        assert new_version["title"] == first["title"]
        assert new_version["state"] == "unsubmitted"
        assert not new_version["submitted"]

# -*- coding: utf-8 -*-

import copy
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

        publish = requests.post(
            create["links"]["publish"], data={"access_token": self.zs.key})

        lookup = self.zs.get_deposition(td["title"])
        assert(lookup["metadata"]["title"] == td["title"])


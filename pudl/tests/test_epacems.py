# -*- coding: utf -8-

import random
import os
from pudl.bin.epacems import EpaCemsFtpManager


class TestEpaCemsFtpManager:

    def test_initialization(self):
        """Make sure the EpaCemsFtpManager loads."""
        EpaCemsFtpManager(testing=True)

    def test_minimal_collection(self):
        """
        Integration:  make sure the EpaCemsFtpManager collects a few files, and
                      cleans up after itself in testing mode
        """
        with EpaCemsFtpManager(testing=True) as cftp:
            year = random.randint(1995, 2019)
            cftp.collect_year(year)

            output_dir = cftp.output_dir
            assert os.path.exists(output_dir)

        assert not os.path.exists(output_dir)

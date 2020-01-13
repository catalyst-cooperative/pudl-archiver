# -*- coding: utf -8-

import os
from pudl.bin.epacems import EpaCemsFtpManager


class TestEpaCemsFtpManager:

    def test_initialization(self):
        """Make sure the EpaCemsFtpManager loads."""
        EpaCemsFtpManager(testing=True)

    def test_cleanup(self):
        """Ensure EpaCemsFtpManager is self-cleaning in test mode."""

        with EpaCemsFtpManager(testing=True) as cftp:
            directory = cftp.output_dir
            assert os.path.exists(directory)

        # The directory only gets deleted in test mode, but it does indicate
        # that the `with` block exited
        assert not os.path.exists(directory)

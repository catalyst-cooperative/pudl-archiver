"""Unit test configuration."""

import os
from pathlib import Path

# Use the checked-in fixture descriptor so unit tests never fetch from S3.
os.environ.setdefault(
    "PUDL_DATAPACKAGE_PATH",
    str(Path(__file__).parent.parent / "fixtures" / "datapackage.json"),
)

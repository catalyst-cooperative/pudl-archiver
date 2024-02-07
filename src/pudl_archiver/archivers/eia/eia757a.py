"""Download EIA 757a data."""

from pudl_archiver.archivers.eia.naturalgas import EiaNGQVArchiver


class Eia757AArchiver(EiaNGQVArchiver):
    """EIA 757A archiver."""

    name = "eia757a"
    form = "757"

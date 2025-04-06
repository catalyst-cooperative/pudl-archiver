"""Download SEC10k extracted tables for arhival."""

import deltalake

from pudl_archiver.archivers.classes import (
    AbstractDatasetArchiver,
    ArchiveAwaitable,
    ResourceInfo,
)

# This can be removed once we change the upstream table names to reflect their raw status
TABLE_NAME_MAP = {
    "core_sec10k__filings": "raw_sec10k__quarterly_filings",
    "core_sec10k__company_information": "raw_sec10k__quarterly_company_information",
    "out_sec10k__parents_and_subsidiaries": "raw_sec10k__parents_and_subsidiaries",
    "core_sec10k__exhibit_21_company_ownership": "raw_sec10k__exhibit_21_company_ownership",
}


class Sec10kArchiver(AbstractDatasetArchiver):
    """Sec10k raw extracted archiver."""

    name = "sec10k"

    async def get_resources(self) -> ArchiveAwaitable:
        """Archive monolithic parquet files for each raw SEC 10k table."""
        for delta_name, raw_name in TABLE_NAME_MAP.items():
            yield self.get_delta_table(delta_name, raw_name)

    async def get_delta_table(self, delta_name: str, raw_name: str):
        """Read configured version of table from deltalake on GCS and save parquet."""
        table_url = f"gs://model-outputs.catalyst.coop/sec10k/{delta_name}"
        download_path = self.download_directory / f"{raw_name}.parquet"
        dt = deltalake.DeltaTable(table_url)
        df = dt.to_pandas()
        df.to_parquet(download_path)

        return ResourceInfo(
            local_path=download_path,
            partitions={"table": raw_name},
        )

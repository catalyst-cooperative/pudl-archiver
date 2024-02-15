"""Cache run details so a run can be resumed after a failure/interruption."""

from pathlib import Path

from pydantic import BaseModel

from pudl_archiver.frictionless import ResourceInfo
from pudl_archiver.zenodo.entities import Deposition

HISTORY_FILE: Path = Path(".run_history.json")


class RunCheckpoint(BaseModel):
    """Model to cache results from a run so it can be resumed after an interruption."""

    deposition: Deposition
    resources: dict[str, ResourceInfo]
    create_new: bool


def save_checkpoint(
    deposition: Deposition, resources: dict[str, ResourceInfo], create_new: bool
):
    """Save current state of run so it can be resumed after an interruption."""
    checkpoint = RunCheckpoint(
        deposition=deposition, resources=resources, create_new=create_new
    )

    with HISTORY_FILE.open("w") as f:
        f.write(checkpoint.model_dump_json(indent=2, by_alias=True))


def load_checkpoint() -> RunCheckpoint:
    """Load run history from file."""
    with HISTORY_FILE.open() as f:
        return RunCheckpoint.model_validate_json(f.read())

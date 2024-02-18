"""Cache run details so a run can be resumed after a failure/interruption."""

from pathlib import Path

from pydantic import BaseModel

from pudl_archiver.frictionless import ResourceInfo
from pudl_archiver.zenodo.entities import Deposition

BASE_PATH = Path(".checkpoints/")


class RunCheckpoint(BaseModel):
    """Model to cache results from a run so it can be resumed after an interruption."""

    dataset: str
    deposition: Deposition
    resources: dict[str, ResourceInfo]
    create_new: bool


def save_checkpoint(
    dataset: str,
    deposition: Deposition,
    resources: dict[str, ResourceInfo],
    create_new: bool,
):
    """Save current state of run so it can be resumed after an interruption."""
    checkpoint = RunCheckpoint(
        dataset=dataset,
        deposition=deposition,
        resources=resources,
        create_new=create_new,
    )

    with (BASE_PATH / f"{dataset}.json").open("w") as f:
        f.write(checkpoint.model_dump_json(indent=2, by_alias=True))


def load_checkpoint(dataset: str) -> RunCheckpoint:
    """Load run history from file."""
    BASE_PATH.mkdir(exist_ok=True)
    with (BASE_PATH / f"{dataset}.json").open() as f:
        return RunCheckpoint.model_validate_json(f.read())

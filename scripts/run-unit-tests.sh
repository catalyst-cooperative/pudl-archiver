#!/usr/bin/env bash
set -euo pipefail

if [[ "${PUDL_ARCHIVER_USE_DOCKER:-0}" == "true" ]]; then
    echo "Running tests in Docker..."
    docker compose run --rm pudl_archiver_pytest tests/unit
else
    echo "Running tests with local Pixi..."
    pixi run pytest tests/unit
fi

FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY src/ src/
COPY scripts/ scripts/
COPY tests/ tests/

COPY pyproject.toml .
COPY pixi.lock .
COPY README.md .
COPY LICENSE.txt .

# Install the pixi environment (includes the editable local package)
RUN pixi install --locked

# Install playwright
RUN pixi run playwright install --with-deps webkit
RUN pixi run playwright install --with-deps chromium

ENTRYPOINT ["pixi", "run", "pudl_archiver"]

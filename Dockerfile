FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY pyproject.toml .
COPY pixi.lock .
COPY README.md .
COPY LICENSE.txt .

# Install the pixi environment (includes the editable local package)
RUN pixi install --locked --environment docker

# Install playwright
RUN pixi run --environment docker playwright install --with-deps webkit
RUN pixi run --environment docker playwright install --with-deps chromium

ENTRYPOINT ["pixi", "run", "pudl_archiver"]

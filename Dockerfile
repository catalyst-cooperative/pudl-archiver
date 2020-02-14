FROM python:alpine

ENV USER=pudl
ENV UID=50000
ENV GID=50000
ENV HOME=/home/pudl
ENV PATH=$PATH:${HOME}/.local/bin

RUN apk add build-base musl-dev python3-dev libffi-dev openssl-dev libxslt-dev

# Install Python Requirements
WORKDIR /install
COPY ./requirements.txt ./
RUN pip install --prefix /usr/local -r requirements.txt

# Set up local user
RUN addgroup --gid ${GID} ${USER} \
    && adduser \
    --disabled-password \
    --gecos "" \
    --home "${HOME}" \
    --ingroup "${USER}" \
    --uid "${UID}" \
    "${USER}"

WORKDIR ${HOME}

# Install 
COPY ./ ${HOME}/scrapers
RUN chown -R ${USER}:${USER} ${HOME}

USER ${USER}
WORKDIR ${HOME}/scrapers
RUN pip install --prefix ${HOME}/.local ./

CMD ./scrape_everything.sh

# typical (second $UID not a typo):
# docker build ./ --build-arg UID=$UID --build-arg GID=$UID
FROM python:alpine

ARG UID
ARG GID
ENV USER=pudl_scrapers
ENV HOME=/home/pudl_scrapers
ENV PATH=$PATH:${HOME}/.local/bin
ENV PUDL_IN=${HOME}/pudl_scrapers/

RUN apk add build-base musl-dev python3-dev libffi-dev openssl-dev libxslt-dev

# Install Python Requirements
WORKDIR /install
COPY ./requirements.txt ./
RUN pip install --prefix /usr/local -r requirements.txt

# Set up local user
RUN addgroup -g ${GID} ${USER} \
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
RUN mkdir -m 775 -p ${PUDL_IN}

WORKDIR ${HOME}/scrapers
RUN pip install --prefix ${HOME}/.local ./

CMD ./scrape_everything.sh

# Make us able to change version and repo when building
ARG GUNICORN_VERSION="~=20.1"
ARG MMD_REPO=https://github.com/metno/mmd
ARG MMD_VERSION=v3.5

# A transistent build container where we don't care about how many layers we add
# ordering of operations matter as cache can be utilized for known good steps
FROM docker.io/ubuntu:22.04 as builder
# Environment variables available during build time - to avoid cluttingering runtime env.
ARG DST=/dst
ARG DEBIAN_FRONTEND="noninteractive"
# Inherit from above
ARG MMD_REPO
ARG MMD_VERSION

# Install packages needed for building
RUN apt-get -qqy update

RUN apt-get -qqy update && \
    apt-get -qqy install \
      git \
      python3-pip \
      python3-venv \
    && python3 -m pip install pip-tools

COPY . /src
WORKDIR /src
RUN mkdir -p /dst

# Compile pip package
RUN pip-compile --resolver=backtracking pyproject.toml
RUN pip-sync
RUN python3 -m build
RUN cp -av ./dist /dst/

# Download MMD and use local copy of schema (see sed command below)
RUN git config --global advice.detachedHead false
RUN git clone --depth 1 --branch ${MMD_VERSION} ${MMD_REPO} /tmp/mmd && \
    mkdir -p $DST/usr/share/mmd/xslt $DST/usr/share/mmd/xsd && \
    cp -a /tmp/mmd/xslt/* $DST/usr/share/mmd/xslt && \
    cp -a /tmp/mmd/xsd/* $DST/usr/share/mmd/xsd && \
    sed -Ei 's#http\://www.w3.org/2001/(xml.xsd)#\1#g' $DST/usr/share/mmd/xsd/*.xsd && \
    rm -rf /tmp/mmd 

# Place container only files in correct location for later copy
RUN cp -a /src/container/. /dst/


# Start with a fresh container to have sensible container image layers
FROM docker.io/ubuntu:22.04

# Label the container
LABEL no.met.project="s-enda"
LABEL source="https://github.com/metno/container-dmci"
LABEL issues="https://github.com/metno/container-dmci/issues"
LABEL description="Discovery Metadata Catalog Ingestor used in the S-ENDA project."

# Environment variables available during build time - to avoid cluttingering runtime env.
ARG DEBIAN_FRONTEND="noninteractive"
ARG GUNICORN_VERSION

# Set config file for dmci
ENV DMCI_CONFIG=/config.yaml

# Install requirements
RUN apt-get -qqy update && \
    apt-get -qqy install \
      ca-certificates \
      dumb-init \
      git \
      htop \
      libxml2 \
      libxslt1.1 \
      python3-lxml \
      python3-pip \
      python3-wheel \
      wget \
    && rm -rf /var/lib/apt/lists/* && \
    pip install "gunicorn${GUNICORN_VERSION}"

COPY --from=builder /dst/. /

RUN ls -l /dist/*; for PKG in /dist/*.tar.gz; do pip install $PKG; done

# Fix netcdf4 ssl error, occurring when solr-indexer tries to read featureType from the netcdf file
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Default port to expose
EXPOSE 8000

# Override workdirectory, expected to have persistent storage
VOLUME /workdir

# Override archive directory, expected to have persistent storage
VOLUME /archive

# Catch interrupts and send to all sub-processes
ENTRYPOINT ["dumb-init", "--"]

# Start application
CMD gunicorn --worker-class sync --workers 5 --bind 0.0.0.0:8000 wsgi:app --keep-alive 5 --log-level info

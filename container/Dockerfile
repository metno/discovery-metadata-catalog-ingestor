FROM alpine:3.13

# Label me
LABEL no.met.project="s-enda"
LABEL source="https://github.com/metno/container-dmci"
LABEL issues="https://github.com/metno/container-dmci/issues"
LABEL description="Container of Discovery Metadata Catalog Ingestor used in the S-ENDA prosject."

# Make us able to change version and repo when building
ARG DMCI_VERION=main
ARG DMCI_REPO=https://github.com/metno/discovery-metadata-catalog-ingestor

RUN apk add --no-cache \
      ca-certificates \
      dumb-init \
      git \
      libxml2 \
      libxslt \
      # install lxml by package, since it takes forever to build
      py3-lxml \
      py3-pip \
      py3-wheel

# Install our runtime server
RUN pip3 install "gunicorn~=20.1"

# FIXME: At later stage run installation with pip3 directly from source
# RUN pip3 install "git+https://github.com/metno/discovery-metadata-catalog-ingestor@main#dmci=discovery-metadata-catalog-ingestor"

# Install from Git
RUN git clone --depth 1 --branch ${DMCI_VERION} ${DMCI_REPO} /dmci
RUN pip3 install -e /dmci

# Add configuration file
ADD config.yaml /dmci

# Add wsgi start script
ADD wsgi.py /

# Default port to expose
EXPOSE 8000

# Workdirectory, expected to have persistent storage
VOLUME /workdir

# Catch interrupts and send to all sub-processes
ENTRYPOINT ["dumb-init", "--"]

# Start application
CMD gunicorn --worker-class sync --workers 1 --bind 0.0.0.0:8000 wsgi:App --keep-alive 5 --keep-alive 5 --log-level info

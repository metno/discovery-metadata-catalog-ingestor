FROM alpine:3.13

# Label the container
LABEL no.met.project="s-enda"
LABEL source="https://github.com/metno/container-dmci"
LABEL issues="https://github.com/metno/container-dmci/issues"
LABEL description="Discovery Metadata Catalog Ingestor used in the S-ENDA project."

# Make us able to change version and repo when building
ARG DMCI_VERSION=main
ARG DMCI_REPO=https://github.com/metno/discovery-metadata-catalog-ingestor
ARG MMD_REPO=https://github.com/metno/mmd
ARG MMD_VERSION=issue_173

# Set config file for dmci
ENV DMCI_CONFIG=/config.yaml

# Install requirements
RUN apk add --no-cache \
      ca-certificates \
      dumb-init \
      wget \
      htop \
      git \
      libxml2 \
      libxslt \
      # install lxml by package, since it takes forever to build
      py3-lxml \
      py3-pip \
      py3-wheel

# Esure latest version
RUN pip3 install --upgrade setuptools pip

# Install our runtime server
RUN pip3 install "gunicorn~=20.1"

# Install from git
RUN pip3 install "git+${DMCI_REPO}@${DMCI_VERSION}#dmci=discovery-metadata-catalog-ingestor"

# Download MMD
RUN git clone --depth 1 --branch ${MMD_VERSION} ${MMD_REPO} /tmp/mmd; \
    mkdir -p /usr/share/mmd/xslt /usr/share/mmd/xsd; \
    cp /tmp/mmd/xslt/* /usr/share/mmd/xslt; \
    cp /tmp/mmd/xsd/* /usr/share/mmd/xsd; \
    rm -rf /tmp/mmd

# Use local copy of schema
RUN sed -Ei 's#http\://www.w3.org/2001/(xml.xsd)#\1#g' /usr/share/mmd/xsd/*.xsd

# Fetch schema
RUN wget -O /usr/share/mmd/xsd/xml.xsd http://www.w3.org/2001/xml.xsd

# Add configuration file
ADD config.yaml /

# Add wsgi start script
ADD wsgi.py /

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

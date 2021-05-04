# dmci, discovery metadata catalog ingestor

Containerization of https://github.com/metno/discovery-metadata-catalog-ingestor.

## Supported tags

* `latest` --- build of latest `main` branch.

##

* ``

## Volumes

* `/workdir` --- directory for persistent storage for the work queue.
* `/dmci/config.yaml` --- mount in your own configuration file.

## Build

```bash
podman  build -t dmci .
```

## Run

```bash
mkdir workdir
podman run --rm -ti -p 8000:8000 -v $(pwd)/workdir:/workdir localhost/dmci:latest
```

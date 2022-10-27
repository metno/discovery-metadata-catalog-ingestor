# dmci, discovery metadata catalog ingestor

Containerization of https://github.com/metno/discovery-metadata-catalog-ingestor. By default this container uses a configuration file which is included in the container. Override it by mounting in your own.

Default `config.yaml`:

```yaml
---
dmci:
  distributors:
    - file
    - pycsw
  distributor_cache: /workdir
  max_permitted_size: 100000
  mmd_xsl_path: /usr/share/mmd/xslt/mmd-to-geonorge.xsl
  mmd_xsd_path: /usr/share/mmd/xsd/mmd.xsd

pycsw:
  csw_service_url: http://localhost

file:
  file_archive_path: /workdir
```

## Supported tags

* `latest` --- build of latest `main` branch.

## Exposes ports

* `8000` --- `dmci` listens on port `8000`.

## Volumes / mounts

* `/workdir` --- temporary storage for work queue.
* `/archive` --- archive directory for persistent storage for the work queue
* `/config.yaml` --- where to mount in configuration file.

## Build

```bash
podman build -t dmci .
```

## Run

```bash
mkdir workdir
podman run --rm -ti -p 8000:8000 -v config.yml:/config.yml:ro -v $(pwd)/workdir:/workdir localhost/dmci:latest
```

# Discovery MetaData Catalog Ingestor

[![flake8](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/syntax.yml/badge.svg?branch=main)](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/syntax.yml)
[![pytest](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/pytest.yml/badge.svg?branch=main)](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/metno/discovery-metadata-catalog-ingestor/branch/main/graph/badge.svg?token=xSG9Sg0jQ0)](https://codecov.io/gh/metno/discovery-metadata-catalog-ingestor)

## Dependencies

For the main packages:

| Package      | PyPi                   | Ubuntu/Debian      | Source                                |
| ------------ | ---------------------- | ------------------ | ------------------------------------- |
| requests     | `pip install requests` | `python3-requests` | https://github.com/psf/requests       |
| pyyaml       | `pip install pyyaml`   | `python3-yaml`     | https://github.com/yaml/pyyaml        |
| flask        | `pip install flask`    | `python3-flask`    | https://github.com/pallets/flask      |
| lxml         | `pip install lxml`     | `python3-lxml`     | https://github.com/lxml/lxml          |

In addition, the tool `pythesint` must be installed from
[github.com/metno/py-thesaurus-interface](https://github.com/metno/py-thesaurus-interface).

The requirements can also be installed with:
```bash
pip install -r requirements.txt
```

## Environment Variables

The package reads the following environment variables.

* `DMCI_CONFIG` should point to the config file, in yaml format.
* `DMCI_LOGFILE` can be set to enable logging to file.
* `DMCI_LOGLEVEL` can be set to change log level. See the Debugging section below.

If the config variable is not set, the package will look for a file named `config.yaml` at the
package root location. If neither the environment variable is set, or this file exists, the
initialisation will fail and exit with exit code `1`.

## Tests

The tests use `pytest`. To run all tests for all modules, run:
```bash
python -m pytest -vv
```

To add coverage, and to optionally generate a coverage report in HTML, run:
```bash
python -m pytest -vv --cov=dmci --cov-report=term --cov-report=html
```
Coverage requires the `pytest-cov` package.

## Debugging

To increase logging level to include info and debug messages, set the environment variable
`DMCI_LOGLEVEL` to the desired level. Valid levels are `CRITICAL`, `ERROR`, `WARNING`, `INFO`, and
`DEBUG`.

## Usage

First initialize the config.yaml or use environment-variables. Then, to start the API run:

```python
python dmci_start_api.py
```

Then you can post with curl to the api - endpoint:

```bash
curl --data "@<PATH_TO_MMD_FILE>" localhost:5000/v1/insert
```
insert is currently the only implemented command. Other commands will return 404 Not Found until implementation.

The API uses HTML return codes, and expected returns are:

    200 for validated and queued requests.
    404 for non-implemented commands.
    413 for files being bigger than treshold (Default is 10MB, given as max_permitted_size)
    500 for validation errors and other internal server problems
    507 if file could not be saved to the work queue

## Licence

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.

See Also: [LICENSE](https://raw.githubusercontent.com/metno/discovery-metadata-catalog-ingestor/main/LICENSE)

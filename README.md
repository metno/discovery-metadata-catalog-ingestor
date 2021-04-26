# Discovery MetaData Catalog Ingestor

[![flake8](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/syntax.yml/badge.svg?branch=main)](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/syntax.yml)
[![pytest](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/pytest.yml/badge.svg?branch=main)](https://github.com/metno/discovery-metadata-catalog-ingestor/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/metno/discovery-metadata-catalog-ingestor/branch/main/graph/badge.svg?token=xSG9Sg0jQ0)](https://codecov.io/gh/metno/discovery-metadata-catalog-ingestor)

## Tests

The tests use `pytest`. To run all tests for all modules, run:
```bash
pytest-3 -vv
```

To add coverage, and to optionally generate a coverage report in HTML, run:
```bash
pytest-3 -vv --cov=dmci --cov-report=term --cov-report=html
```

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

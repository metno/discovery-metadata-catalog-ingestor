"""
DMCI : Api Test
=================

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import dmci.api.api as api

import pytest


@pytest.fixture
def client():
    #FIX ME: Find out how to properly use the init in dmci/api

    with api.app.test_client() as client:
        yield client


@pytest.mark.core
def test_api(client):
    assert client.get("/").status_code == 405
    assert client.post("/").status_code == 200


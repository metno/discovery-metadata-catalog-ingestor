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

import os
import pytest

from tools import causeOSError, readFile

from dmci.api import App

@pytest.fixture(scope="function")
def client(tmpDir, tmpConf):
    """Create an instance of the API.
    """
    workDir = os.path.join(tmpDir, "api")
    os.mkdir(workDir)

    app = App()
    app._conf = tmpConf
    app._conf.distributor_input_path = workDir

    with app._app.test_client() as client:
        yield client

    return

@pytest.mark.api
def testApiApp_Requests(client, filesDir, monkeypatch):
    """Test api requests.
    """
    assert client.get("/").status_code == 404
    assert client.get("/v1/").status_code == 405

    mmdFile = os.path.join(filesDir, "api", "test.xml")
    xmlFile = readFile(mmdFile)

    wrongXmlFile = "<xml: notXml"

    assert client.post("/v1/", data=xmlFile).status_code == 200
    assert client.post("/v1/", data=wrongXmlFile).status_code == 500

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/v1/", data=xmlFile).status_code == 507

# END Test testApiApp_Requests

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
import flask

from tools import causeOSError, readFile

from dmci.api import App
from dmci.api.worker import Worker

@pytest.fixture(scope="function")
def client(tmpDir, tmpConf, monkeypatch):
    """Create an instance of the API.
    """
    workDir = os.path.join(tmpDir, "api")
    if not os.path.isdir(workDir):
        os.mkdir(workDir)

    monkeypatch.setattr("dmci.CONFIG", tmpConf)
    tmpConf.distributor_input_path = workDir

    app = App()
    assert app._conf.distributor_input_path == workDir

    with app.test_client() as client:
        yield client

    return

@pytest.mark.api
def testApiApp_Init(tmpConf, monkeypatch):
    # Test if app fails if distributor_input_path is not given
    with monkeypatch.context() as mp:
        mp.setattr("dmci.CONFIG", tmpConf)
        tmpConf.distributor_input_path = None

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            App()

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1


@pytest.mark.api
def testApiApp_Requests(client):
    assert client.get("/").status_code == 404

# END Test testApiApp_Requests

@pytest.mark.api
def testApiApp_InsertRequests(client, filesDir, monkeypatch):
    """Test api insert-requests.
    """
    assert isinstance(client, flask.testing.FlaskClient)
    assert client.get("/v1/insert").status_code == 405

    mmdFile = os.path.join(filesDir, "api", "test.xml")
    xmlFile = readFile(mmdFile)

    wrongXmlFile = "<xml: notXml"
    # Test sending 3MB of data
    tooLargeXmlFile = bytes(3*1000*1000)

    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, ""))
        assert client.post("/v1/insert", data=xmlFile).status_code == 200
        
    assert client.post("/v1/insert", data=wrongXmlFile).status_code == 500
    assert client.post("/v1/insert", data=tooLargeXmlFile).status_code == 413

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/v1/insert", data=xmlFile).status_code == 507


# END Test testApiApp_InsertRequests

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

from tools import causeOSError

from dmci.api import App

MOCK_XML = b"<xml />"

@pytest.fixture(scope="function")
def client(tmpDir, tmpConf, monkeypatch):
    """Create an instance of the API.
    """
    workDir = os.path.join(tmpDir, "api")
    if not os.path.isdir(workDir):
        os.mkdir(workDir)

    monkeypatch.setattr("dmci.CONFIG", tmpConf)
    tmpConf.distributor_cache = workDir

    app = App()
    assert app._conf.distributor_cache == workDir

    with app.test_client() as client:
        yield client

    return

@pytest.mark.api
def testApiApp_Init(tmpConf, monkeypatch):
    """Test if app fails if distributor_cache is not given
    """
    monkeypatch.setattr("dmci.CONFIG", tmpConf)
    tmpConf.distributor_cache = None

    with pytest.raises(SystemExit) as sysExit:
        App()

    assert sysExit.type == SystemExit
    assert sysExit.value.code == 1

# END Test testApiApp_Init

@pytest.mark.api
def testApiApp_EndPoints(client):
    """Test requests to endpoints not in use.
    """
    assert client.get("/").status_code == 404
    assert client.get("/v1/").status_code == 404

    assert client.get("/v1/insert").status_code == 405

    # should return 405 when implemented
    assert client.get("/v1/update").status_code == 404
    assert client.get("/v1/delete").status_code == 404

    # When implemented should be tested in own functions with
    assert client.post("/v1/update").status_code == 404
    assert client.post("/v1/delete").status_code == 404

# END Test testApiApp_EndPoints

@pytest.mark.api
def testApiApp_InsertRequests(client, filesDir, monkeypatch):
    """Test api insert requests.
    """
    assert isinstance(client, flask.testing.FlaskClient)

    # Test sending 3MB of data
    tooLargeFile = bytes(3000000)
    assert client.post("/v1/insert", data=tooLargeFile).status_code == 413

    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, ""))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 200

    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (False, ""))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 500

# END Test testApiApp_InsertRequests

@pytest.mark.api
def testApiApp_PersistFile(tmpDir, monkeypatch):
    """Test the persistent file writer function.
    """
    assert App._persist_file(MOCK_XML, None)[1] == 507

    outFile = os.path.join(tmpDir, "app_persist_file.xml")

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert App._persist_file(MOCK_XML, outFile)[1] == 507
        assert not os.path.isfile(outFile)

    assert App._persist_file(MOCK_XML, outFile)[1] == 200
    assert os.path.isfile(outFile)

# END Test testApiApp_PersistFile

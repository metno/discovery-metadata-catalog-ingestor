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

from tools import causeOSError, readFile, writeFile

from dmci.api import App

MOCK_XML = b"<xml />"


@pytest.fixture(scope="function")
def client(tmpDir, tmpConf, mockXsd, monkeypatch):
    """Create an instance of the API."""
    workDir = os.path.join(tmpDir, "api")
    rejectDir = os.path.join(tmpDir, "api", "rejected")
    if not os.path.isdir(workDir):
        os.mkdir(workDir)

    monkeypatch.setattr("dmci.CONFIG", tmpConf)
    tmpConf.distributor_cache = workDir
    tmpConf.rejected_jobs_path = rejectDir
    tmpConf.mmd_xsd_path = mockXsd

    app = App()
    assert app._conf.distributor_cache == workDir

    with app.test_client() as client:
        yield client

    return


@pytest.mark.api
def testApiApp_Init(tmpConf, tmpDir, monkeypatch):
    """Test if app fails if distributor_cache and mmd_xsd_path are not
    given in the config.
    """
    monkeypatch.setattr("dmci.CONFIG", tmpConf)

    tmpConf.distributor_cache = None
    tmpConf.mmd_xsd_path = None
    with pytest.raises(SystemExit) as sysExit:
        App()

    assert sysExit.type == SystemExit
    assert sysExit.value.code == 1

    tmpConf.distributor_cache = tmpDir
    tmpConf.mmd_xsd_path = None
    with pytest.raises(SystemExit) as sysExit:
        App()

    assert sysExit.type == SystemExit
    assert sysExit.value.code == 1

    # Check Invalid XML Schema
    failXsd = os.path.join(tmpDir, "app_invalid.xsd")
    writeFile(failXsd, "blablabla")
    tmpConf.mmd_xsd_path = failXsd
    with pytest.raises(SystemExit) as sysExit:
        App()

# END Test testApiApp_Init


@pytest.mark.api
def testApiApp_EndPoints(client):
    """Test requests to endpoints not in use."""
    assert client.get("/").status_code == 404
    assert client.get("/v1/").status_code == 404

    assert client.get("/v1/insert").status_code == 405
    assert client.get("/v1/update").status_code == 405

    # should return 405 when implemented
    assert client.get("/v1/delete").status_code == 404

    # When implemented should be tested in own functions with
    # assert client.post("/v1/update").status_code == 404
    assert client.post("/v1/delete").status_code == 404

# END Test testApiApp_EndPoints


@pytest.mark.api
def testApiApp_InsertUpdateRequests(client, monkeypatch):
    """Test api insert and update requests."""
    assert isinstance(client, flask.testing.FlaskClient)

    # Test sending 3MB of data
    tooLargeFile = bytes(3000000)
    assert client.post("/v1/insert", data=tooLargeFile).status_code == 413
    assert client.post("/v1/update", data=tooLargeFile).status_code == 413

    # Fail cahcing the file
    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 507
        assert client.post("/v1/update", data=MOCK_XML).status_code == 507

    # Data is valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, ""))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 200
        assert client.post("/v1/update", data=MOCK_XML).status_code == 200

    # Data is not valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (False, ""))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 400
        assert client.post("/v1/update", data=MOCK_XML).status_code == 400

    # Data is valid, distribute fails
    with monkeypatch.context() as mp:
        f = ["A", "B"]
        s = ["C"]
        e = ["Reason A", "Reason B"]
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, ""))
        mp.setattr("dmci.api.app.Worker.distribute", lambda *a: (False, False, [], f, s, e))

        response = client.post("/v1/insert", data=MOCK_XML)
        assert response.status_code == 500
        assert response.data == (
            b"The following distributors failed: A, B\n"
            b" - A: Reason A\n"
            b" - B: Reason B\n"
            b"The following jobs were skipped: C"
        )

        response = client.post("/v1/update", data=MOCK_XML)
        assert response.status_code == 500
        assert response.data == (
            b"The following distributors failed: A, B\n"
            b" - A: Reason A\n"
            b" - B: Reason B\n"
            b"The following jobs were skipped: C"
        )

# END Test testApiApp_InsertRequests


@pytest.mark.api
def testApiApp_PersistFile(tmpDir, monkeypatch):
    """Test the persistent file writer function."""
    assert App._persist_file(MOCK_XML, None)[1] == 507

    outFile = os.path.join(tmpDir, "app_persist_file.xml")

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert App._persist_file(MOCK_XML, outFile)[1] == 507
        assert not os.path.isfile(outFile)

    assert App._persist_file(MOCK_XML, outFile)[1] == 200
    assert os.path.isfile(outFile)

# END Test testApiApp_PersistFile


@pytest.mark.api
def testApiApp_HandlePersistFile(caplog, fncDir, monkeypatch):
    """Test the persistent file handler function."""
    rejectDir = os.path.join(fncDir, "rejected")
    testFile = os.path.join(fncDir, "test.xml")
    rejectFile = os.path.join(rejectDir, "test.xml")
    errorFile = os.path.join(rejectDir, "test.txt")

    os.mkdir(rejectDir)
    assert os.path.isdir(rejectDir)

    # Status OK
    # =========

    # Invalid full_path
    caplog.clear()
    assert App._handle_persist_file(True, None) is False
    assert "Failed to unlink processed file" in caplog.text

    # Valid full path, and delete
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    assert App._handle_persist_file(True, testFile) is True
    assert not os.path.isfile(testFile)

    # Status NOK
    # ==========

    # Fail move to rejected
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    caplog.clear()
    assert App._handle_persist_file(False, testFile, None, "Error") is False
    assert "Failed to move persist file to rejected folder" in caplog.text
    assert os.path.isfile(testFile)
    assert not os.path.isfile(rejectFile)
    assert not os.path.isfile(errorFile)

    # Successful move to rejected
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    assert App._handle_persist_file(False, testFile, rejectFile, "Error") is True
    assert not os.path.isfile(testFile)
    assert os.path.isfile(rejectFile)
    assert os.path.isfile(errorFile)
    assert readFile(errorFile) == "Error"

    # Fail writing error file
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    caplog.clear()
    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert App._handle_persist_file(False, testFile, rejectFile, "Error") is False
        assert "Failed to write rejected reason to file" in caplog.text

# END Test testApiApp_HandlePersistFile

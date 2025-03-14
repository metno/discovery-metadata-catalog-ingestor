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
import uuid
import pytest
import flask

from tools import readFile
from tools import writeFile

from tools import causeOSError
from tools import causePermissionError
from tools import causeSameFileError
from tools import causeShUtilError

from prometheus_client import REGISTRY

from dmci.api import App

MOCK_XML = b"<xml />"
MOCK_XML_MOD = b"<xml mod />"


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
    tmpConf.path_to_parent_list = mockXsd

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

    # Get method is not allowed
    assert client.get("/v1/insert").status_code == 405
    assert client.get("/v1/update").status_code == 405
    assert client.get("/v1/validate").status_code == 405

    # Bare delete command is not allowed
    assert client.post("/v1/delete").status_code == 404


# END Test testApiApp_EndPoints


class MockException:
    def __init__(self, *args, **kwargs):
        raise Exception


@pytest.mark.api
def testApiApp_EndPoints_Exception(tmpDir, tmpConf, mockXsd, monkeypatch):
    """Test requests to endpoints Exception."""
    workDir = os.path.join(tmpDir, "api")
    rejectDir = os.path.join(tmpDir, "api", "rejected")
    if not os.path.isdir(workDir):
        os.mkdir(workDir)
    if not os.path.isdir(rejectDir):
        os.mkdir(rejectDir)
    monkeypatch.setattr("dmci.CONFIG", tmpConf)

    tmpConf.distributor_cache = workDir
    tmpConf.rejected_jobs_path = rejectDir
    tmpConf.mmd_xsd_path = mockXsd
    tmpConf.path_to_parent_list = mockXsd
    monkeypatch.setattr("lxml.etree.XMLSchema", MockException)
    with pytest.raises(SystemExit):
        App()


# END Test testApiApp_EndPoints_Exception


@pytest.mark.api
def testApiApp_InsertUpdateRequests(client, monkeypatch):
    """Test api insert and update requests."""
    assert isinstance(client, flask.testing.FlaskClient)

    # Test sending 3MB of data
    tooLargeFile = bytes(3000000)
    assert client.post("/v1/insert", data=tooLargeFile).status_code == 413
    assert client.post("/v1/update", data=tooLargeFile).status_code == 413

    # Test sending 0B of data
    noFile = bytes(0)
    assert client.post("/v1/insert", data=noFile).status_code == 202
    assert client.post("/v1/update", data=noFile).status_code == 202

    # Fail caching the file
    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 507
        assert client.post("/v1/update", data=MOCK_XML).status_code == 507

    # Data is valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 200
        assert client.post("/v1/update", data=MOCK_XML).status_code == 200

        # Data is valid and gets modified by validate
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML_MOD))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 200
        assert client.post("/v1/update", data=MOCK_XML).status_code == 200

    # first _persist_file fails
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        mp.setattr(
            "dmci.api.app.App._persist_file",
            lambda *a: ("Failed to write the file", 666),
        )
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 666
        assert client.post("/v1/update", data=MOCK_XML).status_code == 666

    # first _persist_file works
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        mp.setattr(
            "dmci.api.app.App._persist_file", lambda *a: ("Everything is OK", 200)
        )
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 200
        assert client.post("/v1/update", data=MOCK_XML).status_code == 200

    # Data is not valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (False, "", MOCK_XML))
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 400
        assert client.post("/v1/update", data=MOCK_XML).status_code == 400

    # Data is valid, distribute fails
    with monkeypatch.context() as mp:
        f = ["A", "B"]
        s = ["C"]
        e = ["Reason A", "Reason B"]
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        mp.setattr(
            "dmci.api.app.Worker.distribute", lambda *a: (False, False, [], f, s, e)
        )

        response = client.post("/v1/insert", data=MOCK_XML)
        assert response.status_code == 500
        assert response.data == (
            b"The following distributors failed: A, B\n"
            b" - A: Reason A\n"
            b" - B: Reason B\n"
            b"The following jobs were skipped: C\n"
        )

        response = client.post("/v1/update", data=MOCK_XML)
        assert response.status_code == 500
        assert response.data == (
            b"The following distributors failed: A, B\n"
            b" - A: Reason A\n"
            b" - B: Reason B\n"
            b"The following jobs were skipped: C\n"
        )

    # Distribution fails, metrics are incremented
    with monkeypatch.context() as mp:
        f = ["file", "solr", "pycsw"]
        s = ["C"]
        e = ["Reason A", "Reason B", "Reason C"]
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        mp.setattr(
            "dmci.api.app.Worker.distribute", lambda *a: (False, False, [], f, s, e)
        )

        response = client.post("/v1/insert", data=MOCK_XML)
        after_file = REGISTRY.get_sample_value(
            "failed_file_dist_total", {"path": "/v1/insert"}
        )
        after_csw = REGISTRY.get_sample_value(
            "failed_pycsw_dist_total", {"path": "/v1/insert"}
        )
        after_solr = REGISTRY.get_sample_value(
            "failed_solr_dist_total", {"path": "/v1/insert"}
        )
        assert after_file == 1
        assert after_csw == 1
        assert after_solr == 1

        response = client.post("/v1/update", data=MOCK_XML)
        after_file = REGISTRY.get_sample_value(
            "failed_file_dist_total", {"path": "/v1/update"}
        )
        after_csw = REGISTRY.get_sample_value(
            "failed_pycsw_dist_total", {"path": "/v1/update"}
        )
        after_solr = REGISTRY.get_sample_value(
            "failed_solr_dist_total", {"path": "/v1/update"}
        )
        assert after_file == 1
        assert after_csw == 1
        assert after_solr == 1

    # Data is valid, distribute OK.
    with monkeypatch.context() as mp:

        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        mp.setattr(
            "dmci.api.app.Worker.distribute", lambda *a: (True, True, [], [], [], [])
        )
        response = client.post("/v1/insert", data=MOCK_XML)
        assert response.status_code == 200
        assert response.data == (b"Everything is OK\n")


# END Test testApiApp_InsertUpdateRequests


@pytest.mark.api
def testApiApp_PersistAgainAfterModification(client, monkeypatch):

    outputs = iter(
        [
            ("Everything is OK", 200),
            ("Failure in persisting", 666),
            ("Everything is OK", 200),
            ("Failure in persisting", 666),
        ]
    )

    @staticmethod
    def fake_output(data, full_path):
        return next(outputs)

    with monkeypatch.context() as mp:
        # Data is valid but failure to persist again after modifications
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML_MOD))
        mp.setattr("dmci.api.app.App._persist_file", fake_output)
        assert client.post("/v1/insert", data=MOCK_XML).status_code == 666
        assert client.post("/v1/update", data=MOCK_XML).status_code == 666


# END Test testApiApp_PersistAgainAfterModification


@pytest.mark.api
def testApiApp_DeleteRequests(client, monkeypatch):
    """Test api delete request."""
    assert isinstance(client, flask.testing.FlaskClient)
    testUUID = "test:7278888a-96a5-4ee5-845a-2051bb8994c8"

    # Invalid UUID
    assert client.post("/v1/delete/blabla").status_code == 400
    # Valid UUID, but no namespace
    assert client.post("/v1/delete/7278888a96a54ee5845a2051bb8994c8").status_code == 400

    # Distribute fails
    with monkeypatch.context() as mp:
        f = ["A", "B"]
        s = ["C"]
        e = ["Reason A", "Reason B"]
        mp.setattr(
            "dmci.api.app.Worker.distribute", lambda *a: (False, False, [], f, s, e)
        )

        response = client.post("/v1/delete/%s" % testUUID, data=MOCK_XML)
        assert response.status_code == 500
        assert response.data == (
            b"The following distributors failed: A, B\n"
            b" - A: Reason A\n"
            b" - B: Reason B\n"
            b"The following jobs were skipped: C\n"
        )

    # Distribute ok
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.api.app.Worker.distribute", lambda *a: (True, True, [], [], [], [])
        )
        response = client.post("/v1/delete/%s" % testUUID, data=MOCK_XML)
        assert response.status_code == 200
        assert response.data == b"Everything is OK\n"


# END Test testApiApp_DeleteRequests


@pytest.mark.api
def testApiApp_ValidateRequests(client, monkeypatch):
    """Test api validate request."""
    assert isinstance(client, flask.testing.FlaskClient)

    # Test sending 3MB of data
    tooLargeFile = bytes(3000000)
    assert client.post("/v1/validate", data=tooLargeFile).status_code == 413

    # Fail caching the file
    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/v1/validate", data=MOCK_XML).status_code == 507

    # Data is valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (True, "", MOCK_XML))
        assert client.post("/v1/validate", data=MOCK_XML).status_code == 200

    # Data is not valid
    with monkeypatch.context() as mp:
        mp.setattr("dmci.api.app.Worker.validate", lambda *a: (False, "", MOCK_XML))
        assert client.post("/v1/validate", data=MOCK_XML).status_code == 400


# END Test testApiApp_ValidateRequests


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
def testApiApp_CheckMetadataId():
    testUUID = "7278888a-96a5-4ee5-845a-2051bb8994c8"
    correct_UUID = uuid.UUID(testUUID)

    # Correct with namespace
    assert App._check_metadata_id("test:" + testUUID) == ("test", correct_UUID, None)

    # Without namespace
    assert App._check_metadata_id(testUUID) == (
        None,
        None,
        "Input must be structured as <namespace>:<uuid>.",
    )
    # With namespace, but not in accordance with the defined env_string
    assert App._check_metadata_id("test:" + testUUID, env_string="TEST") == (
        None,
        None,
        "Dataset metadata_id " "namespace is wrong: " "test",
    )

    # With namespace but the wrong one for prod environement
    assert App._check_metadata_id("test.dev:"+testUUID, env_string=None) == (None, None,
                                                                             "Dataset metadata_id "
                                                                             "namespace is wrong "
                                                                             "for production: "
                                                                             "test.dev")
    assert App._check_metadata_id("test.staging:"+testUUID) == (None, None,
                                                                "Dataset metadata_id "
                                                                "namespace is wrong "
                                                                "for production: "
                                                                "test.staging")

    # With namespace, defined env_string, but present in call
    assert App._check_metadata_id("test.TEST:" + testUUID, env_string="TEST") == (
        "test.TEST",
        correct_UUID,
        None,
    )

    # Test with namespace, but malformed UUID
    out = App._check_metadata_id("test:blabla")
    assert out[0] is None
    assert out[1] is None
    assert out[2] == "Cannot convert to UUID: blabla"

    out = App._check_metadata_id("test:wrong:7278888a96a54ee5845a2051bb8994c8")
    assert out[0] is None
    assert out[1] is None
    assert out[2] == "Input must be structured as <namespace>:<uuid>."


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

    # Fail move to rejected samefile error
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    caplog.clear()
    with monkeypatch.context() as mp:
        mp.setattr("shutil.copy", causeSameFileError)
        assert App._handle_persist_file(False, testFile, testFile, "Error") is False
        assert "Source and destination represents the same file" in caplog.text

    # Fail move to rejected permission error
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    caplog.clear()
    with monkeypatch.context() as mp:
        mp.setattr("shutil.copy", causePermissionError)
        assert App._handle_persist_file(False, testFile, rejectFile, "Error") is False
        assert "Permission denied" in caplog.text

    # Fail move to rejected generic error
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    caplog.clear()
    with monkeypatch.context() as mp:
        mp.setattr("shutil.copy", causeShUtilError)
        assert App._handle_persist_file(False, testFile, rejectFile, "Error") is False
        assert "Something failed moving the rejected file" in caplog.text

    # Successful move to rejected
    writeFile(testFile, "<xml />")
    assert os.path.isfile(testFile)
    assert App._handle_persist_file(False, testFile, rejectFile, "Error") is True
    assert not os.path.isfile(testFile)
    assert os.path.isfile(rejectFile)
    assert os.path.isfile(errorFile)
    assert readFile(errorFile) == "Error"


@pytest.mark.api
def testApiApp_HandlePersistFile_fail2write_reason(caplog, fncDir, monkeypatch):
    """Test that _handle_persist_file catches the error if it fails
    to open the reject file (that should provide the reason for
    failing to write the persist file to the rejected folder).
    """
    import builtins

    # Fail writing error file
    rejectDir = os.path.join(fncDir, "rejected")
    full_path = os.path.join(fncDir, "test.xml")
    reject_path = os.path.join(rejectDir, "test.xml")

    os.mkdir(rejectDir)
    assert os.path.isdir(rejectDir)

    writeFile(full_path, "<xml />")
    assert os.path.isfile(full_path)
    caplog.clear()

    original_open = builtins.open

    def patched_open(*args, **kwargs):
        if "rejected/test.xml" in args[0]:
            mp.setattr("builtins.open", causeOSError)

        return original_open(*args, **kwargs)

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", patched_open)
        assert (
            App._handle_persist_file(False, full_path, reject_path, "Some reason")
            is False
        )
        assert "Failed to write rejected reason to file" in caplog.text


# END Test testApiApp_HandlePersistFile

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


from tools import causeOSError
import pytest
import os

from dmci.api import App


@pytest.fixture(scope="module")
def client(tmpDir):
    workDir = os.path.join(tmpDir, "api")
    os.mkdir(workDir)
    app = App()
    app._conf.distributor_input_path = workDir

    with app._app.test_client() as client:
        yield client


@pytest.mark.api
def testApiApi_requests(client, refDir, monkeypatch):
    assert client.get("/").status_code == 405

    mmdFile = os.path.join(refDir, "api", "test.xml")
    with open(mmdFile, "rb") as infile:
        xmlFile = infile.read()

    wrongXmlFile = "<xml: notXml"

    assert client.post("/", data=xmlFile).status_code == 200
    assert client.post("/", data=wrongXmlFile).status_code == 500

    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert client.post("/", data=xmlFile).status_code == 507

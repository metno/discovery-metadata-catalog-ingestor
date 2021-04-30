# -*- coding: utf-8 -*-
"""
DMCI : Worker Class Test
========================

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

from tools import writeFile, readFile

from dmci.api.worker import Worker

@pytest.mark.api
def testApiWorker_Init():
    """Test the Worker class init.
    """
    assert Worker(None)._dist_cmd == "insert"
    assert Worker(None, a=1)._kwargs == {"a": 1}

# END Test testApiWorker_Init

@pytest.mark.api
def testApiWorker_Distributor(tmpDir, tmpConf, monkeypatch):
    """Test the Worker class distributor.
    """
    workDir = os.path.join(tmpDir, "worker")
    os.mkdir(workDir)

    # Create a test config file and object
    workConf = os.path.join(workDir, "distributor_config.yaml")
    writeFile(workConf, (
        "dmci:\n"
        "  distributors:\n"
        "    - git\n"
        "    - blabla\n"
    ))

    tmpConf.readConfig(workConf)
    assert tmpConf.call_distributors == ["git", "blabla"]

    # Create a dummy xml file
    dummyXml = os.path.join(workDir, "dummy.xml")
    writeFile(dummyXml, "<xml />")

    # Call the distributor function with the distributors from the config
    tstWorker = Worker(None)
    tstWorker._conf = tmpConf
    tstWorker._dist_xml_file = dummyXml

    status, valid, called, failed, skipped = tstWorker.distribute()
    assert status is True
    assert valid is True
    assert called == ["git"]
    assert failed == []
    assert skipped == ["blabla"]

    # Call the distributor function with the wrong parameters
    tstWorker = Worker(None)
    tstWorker._conf = tmpConf
    tstWorker._dist_cmd = "blabla"
    tstWorker._dist_xml_file = "/path/to/nowhere"

    status, valid, _, _, _ = tstWorker.distribute()
    assert status is False
    assert valid is False

# END Test testApiWorker_Distributor

@pytest.mark.api
def testApiWorker_Validator(monkeypatch, filesDir):
    """Test the Worker class validator.
    """
    fn = os.path.join(filesDir, "api", "passing.xml")
    data = readFile(fn)

    assert Worker(fn).validate(data) == (False, "input must be bytes type")

    data = bytes(readFile(fn), 'utf-8')
    monkeypatch.setattr(Worker, '_check_information_content', lambda *a: (True, ""))
    assert Worker(fn).validate(data) == (True, "")

# END Test testApiWorker_Validator

@pytest.mark.api
def testApiWorker_CheckInfoContent(filesDir):
    """Test _check_information_content
    """
    fn = os.path.join(filesDir, "api", "passing.xml")
    data = readFile(fn)
    assert Worker(fn)._check_information_content(data) == (False, "input must be bytes type")
    data = bytes(readFile(fn), "utf-8")
    assert Worker(fn)._check_information_content(data) == (True, "Input MMD xml file is ok")

# END Test testApiWorker_CheckInfoContent

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
import lxml
import pytest

from tools import readFile

from dmci.api.worker import Worker
from dmci.distributors import FileDist, PyCSWDist

@pytest.mark.api
def testApiWorker_Init():
    """Test the Worker class init.
    """
    assert Worker(None, None)._dist_cmd == "insert"
    assert Worker(None, None, a=1)._kwargs == {"a": 1}

# END Test testApiWorker_Init

@pytest.mark.api
def testApiWorker_Distributor(tmpDir, tmpConf, mockXml, monkeypatch):
    """Test the Worker class distributor.
    """
    tmpConf.call_distributors = ["file", "pycsw", "blabla"]

    # Call the distributor function with the distributors from the config
    with monkeypatch.context() as mp:
        mp.setattr(FileDist, "run", lambda *a: True)
        mp.setattr(PyCSWDist, "run", lambda *a: True)

        tstWorker = Worker(None, None)
        tstWorker._conf = tmpConf
        tstWorker._dist_xml_file = mockXml

        status, valid, called, failed, skipped = tstWorker.distribute()
        assert status is True
        assert valid is True
        assert called == ["file", "pycsw"]
        assert failed == []
        assert skipped == ["blabla"]

    # Call the distributor function with the wrong parameters
    tstWorker = Worker(None, None)
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
    xsdFile = os.path.join(filesDir, "mmd", "mmd.xsd")
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "api", "failing.xml")

    xsdObj = lxml.etree.XMLSchema(lxml.etree.parse(xsdFile))
    passWorker = Worker(passFile, xsdObj)
    failWorker = Worker(failFile, xsdObj)

    # Invalid data format
    passData = readFile(passFile)
    assert passWorker.validate(passData) == (False, "input must be bytes type")

    # Valid data format
    with monkeypatch.context() as mp:
        mp.setattr(Worker, '_check_information_content', lambda *a: (True, ""))

        # Valid XML
        passData = bytes(readFile(passFile), "utf-8")
        valid, msg = passWorker.validate(passData)
        assert valid is True
        assert isinstance(msg, str)
        assert not msg

        # Invalid XML
        failData = bytes(readFile(failFile), "utf-8")
        valid, msg = failWorker.validate(failData)
        assert valid is False
        assert isinstance(msg, str)
        assert msg

# END Test testApiWorker_Validator

@pytest.mark.api
def testApiWorker_CheckInfoContent(monkeypatch, filesDir):
    """Test _check_information_content
    """
    passFile = os.path.join(filesDir, "api", "passing.xml")
    tstWorker = Worker(passFile, None)

    # Invalid data format
    passData = readFile(passFile)
    assert tstWorker._check_information_content(passData) == (False, "input must be bytes type")

    # Valid data format
    with monkeypatch.context() as mp:
        mp.setattr("dmci.mmd_tools.check_mmd.check_url", lambda *a, **k: True)
        passData = bytes(readFile(passFile), "utf-8")
        assert tstWorker._check_information_content(passData) == (
            True, "Input MMD XML file is ok"
        )

    # Valid data format, invalid content
    with monkeypatch.context() as mp:
        mp.setattr("dmci.mmd_tools.check_mmd.check_url", lambda *a, **k: False)
        passData = bytes(readFile(passFile), "utf-8")
        assert tstWorker._check_information_content(passData) == (
            False, "Input MMD XML file contains errors, please check your file"
        )

    # Invalid XML file
    failFile = os.path.join(filesDir, "api", "failing.xml")
    failData = bytes(readFile(failFile), "utf-8")
    assert tstWorker._check_information_content(failData) == (
        False, "Input MMD XML file has no valid UUID metadata_identifier"
    )

# END Test testApiWorker_CheckInfoContent

@pytest.mark.api
def testApiWorker_ExtractMetaDataID(filesDir, mockXml):
    """Test _check_information_content
    """
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "api", "failing.xml")

    # Valid File
    passXML = lxml.etree.fromstring(bytes(readFile(passFile), "utf-8"))
    tstWorker = Worker(passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is True
    assert tstWorker._file_metadata_id is not None

    # Invalid File
    mockData = lxml.etree.fromstring(bytes(readFile(mockXml), "utf-8"))
    tstWorker = Worker(mockXml, None)
    assert tstWorker._extract_metadata_id(mockData) is False
    assert tstWorker._file_metadata_id is None

    # Invalid UUID
    failXML = lxml.etree.fromstring(bytes(readFile(failFile), "utf-8"))
    tstWorker = Worker(failFile, None)
    assert tstWorker._extract_metadata_id(failXML) is False
    assert tstWorker._file_metadata_id is None

# END Test testApiWorker_ExtractMetaDataID

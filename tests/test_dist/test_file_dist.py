"""
DMCI : File Distributor Class Test
==================================

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

from tools import causeOSError, readFile

from dmci.api.worker import Worker
from dmci.distributors import FileDist
from dmci.distributors.distributor import DistCmd


@pytest.mark.dist
def testDistFile_Init():
    """Test the FileDist class init."""
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert FileDist("insert", metadata_id="some_id").is_valid() is False
    assert FileDist("update", metadata_id="some_id").is_valid() is False
    assert FileDist("delete", metadata_id="some_id").is_valid() is True
    assert FileDist("blabla", metadata_id="some_id").is_valid() is False

# END Test testDistFile_Init


@pytest.mark.dist
def testDistFile_Run(mockXml):
    """Test the FileDist class run function."""
    tstDist = FileDist("insert", xml_file=mockXml)
    assert tstDist.is_valid()

    tstDist._valid = False
    assert tstDist.run() is False
    tstDist._valid = True

    tstDist._add_to_archive = lambda *a: True

    tstDist._cmd = DistCmd.INSERT
    assert tstDist.run() is True

    tstDist._cmd = DistCmd.UPDATE
    assert tstDist.run() is True

    tstDist._cmd = DistCmd.DELETE
    assert tstDist.run() is False

    tstDist._cmd = 1234
    assert tstDist.run() is False

# END Test testDistFile_Run


@pytest.mark.dist
def testDistFile_InsertUpdate(tmpDir, filesDir, monkeypatch):
    """Test the FileDist class insert and update actions."""
    fileDir = os.path.join(tmpDir, "file")
    archDir = os.path.join(fileDir, "archive")
    passFile = os.path.join(filesDir, "api", "passing.xml")

    # Set up a Worker object
    passXML = lxml.etree.fromstring(bytes(readFile(passFile), "utf-8"))
    tstWorker = Worker(passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is True
    assert tstWorker._file_metadata_id is not None

    # No file archive path set
    tstDist = FileDist("insert", xml_file=passFile)
    assert tstDist._add_to_archive() is False

    # No identifier set
    tstDist._conf.file_archive_path = archDir
    assert tstDist.run() is False

    # Invalid identifier set
    tstDist._worker = tstWorker
    goodUUID = tstWorker._file_metadata_id
    tstWorker._file_metadata_id = "123456789abcdefghijkl"
    assert tstDist.run() is False

    # Should have a valid identifier from here on
    tstWorker._file_metadata_id = goodUUID

    # Fail the making of folders
    with monkeypatch.context() as mp:
        mp.setattr("os.makedirs", causeOSError)
        assert tstDist.run() is False

    # Fail the copy process
    with monkeypatch.context() as mp:
        mp.setattr("shutil.copy2", causeOSError)
        assert tstDist.run() is False

    # Finally, test a successful write
    assert tstDist.run() is True

    dirA = os.path.join(archDir, "arch_f")
    assert os.path.isdir(dirA)

    dirB = os.path.join(dirA, "arch_0")
    assert os.path.isdir(dirB)

    dirC = os.path.join(dirB, "arch_f")
    assert os.path.isdir(dirC)

    archFile = os.path.join(dirC, "a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b.xml")
    assert os.path.isfile(archFile)

    # Should not be allowed to write the same file twice
    assert tstDist.run() is False

    # Unless command is to update
    tstDist._cmd = DistCmd.UPDATE
    assert tstDist.run() is True

# END Test testDistFile_InsertUpdate

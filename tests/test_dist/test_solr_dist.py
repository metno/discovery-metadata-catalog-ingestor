"""
DMCI : SolR Distributor Class Test
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
import logging

from tools import causeOSError
from tools import causeException
from tools import readFile

from dmci.api.worker import Worker
from dmci.distributors import SolRDist
from dmci.distributors.distributor import DistCmd


class MockIndexMMD:

    def __init__(self, *args, **kwargs):
        pass

    def get_dataset(self, *args, **kwargs):
        is_indexed = {
            'doc': None,
        }
        return is_indexed 

    def index_record(self, *args, **kwargs):
        return True, 'test'

class MockMMD4SolR:

    def __init__(self, *args, **kwargs):
        pass

    def check_mmd(self, *args, **kwargs):
        return None

    def tosolr(self, *args, **kwargs):
        solr_formatted = {
            'id': 'no-met-dev-250ba38f-1081-4669-a429-f378c569db32',
            'metadata_identifier': 'no.met.dev:250ba38f-1081-4669-a429-f378c569db32',
        }
        return solr_formatted


@pytest.mark.dist
def testDistSolR_Init(tmpUUID):
    """Test the SolRDist class init."""
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert SolRDist("insert", metadata_id=tmpUUID).is_valid() is False
    assert SolRDist("update", metadata_id=tmpUUID).is_valid() is False
    assert SolRDist("delete", metadata_id=tmpUUID).is_valid() is True
    assert SolRDist("blabla", metadata_id=tmpUUID).is_valid() is False

# END Test testDistSolR_Init


@pytest.mark.dist
def testDistSolR_Run(mockXml, tmpUUID, monkeypatch):
    """Test the SolRDist class run function."""

    # Initialise object, and check that it validates
    tstDist = SolRDist("insert", xml_file=mockXml)
    assert tstDist.is_valid()

    # Invalidate object and assert that the run function returns the
    # expected tuple
    tstDist._valid = False
    assert tstDist.run() == (False, "The run job is invalid")

    tstDist._valid = True

    # Mock functions called by SolRDist.run
    tstDist._add = lambda *a: (True, "test")
    tstDist._delete = lambda *a: (True, "test")

    tstDist._cmd = DistCmd.INSERT
    assert tstDist.run() == (True, "test")

    tstDist._cmd = DistCmd.UPDATE
    assert tstDist.run() == (True, "test")

    tstDist._cmd = DistCmd.DELETE
    assert tstDist.run() == (True, "test")

    tstDist._cmd = 1234
    assert tstDist.run() == (False, "No job was run")


@pytest.mark.dist
def testDistSolR_add_MMD4SolR_raises_exception(mockXml, monkeypatch):
    """ Test _add function failing on initialization of MMD4SolR
    instance.
    """
    # Initialise object, and check that it validates
    tstDist = SolRDist("insert", xml_file=mockXml)
    assert tstDist.is_valid()

    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR", causeException)
        assert tstDist._add() == (
            False, "Could not read file %s: Test Exception" % mockXml
        )
        

@pytest.mark.dist
def testDistSolR_add_successful(mockXml, monkeypatch):
    """ Test that the _add function successfully completes with the
    correct return message.
    """
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR",
            lambda *args, **kwargs: MockMMD4SolR(*args, **kwargs))
        mp.setattr("dmci.distributors.solr_dist.IndexMMD",
            lambda *args, **kwargs: MockIndexMMD(*args, **kwargs))

        # Initialise object, and check that it validates
        tstDist = SolRDist("insert", xml_file=mockXml)
        assert tstDist.is_valid()
        assert tstDist._add() == (
            True, "test"
        )


@pytest.mark.dist
def testDistSolR_add_tosolr_raises_exception(mockXml, monkeypatch):
    """ Test that the _add function fails correctly when
    MMD4SolR.tosolr raises an exception.
    """
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR",
            lambda *args, **kwargs: MockMMD4SolR(*args, **kwargs))
        mp.setattr(MockMMD4SolR, "tosolr", causeException)
        tstDist = SolRDist("insert", xml_file=mockXml)
        assert tstDist._add() == (
            False, "Could not process the file %s: Test Exception" % mockXml
        )
        #tstDist._add()
        #assert tstDist._add() == (
        #    False, "Could not read file %s: Test Exception" % mockXml
        #)


@pytest.mark.dist
def testDistSolR_Delete(tmpDir, filesDir, monkeypatch):
    """Test the SolRDist class insert and update actions."""
    fileDir = os.path.join(tmpDir, "file_delete")
    archDir = os.path.join(fileDir, "archive")
    passFile = os.path.join(filesDir, "api", "passing.xml")

    # Set up a Worker object
    passXML = lxml.etree.fromstring(bytes(readFile(passFile), "utf-8"))
    tstWorker = Worker("insert", passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is True
    assert tstWorker._file_metadata_id is not None

    tstDist = SolRDist("insert", xml_file=passFile)
    tstDist._conf.file_archive_path = archDir
    tstDist._worker = tstWorker

    # Insert a new file to delete
    tstDist._cmd = DistCmd.INSERT
    assert tstDist.run() == (
        True, "Added file: a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b.xml"
    )

    # Try to delete with no identifier set
    tstDist._cmd = DistCmd.DELETE
    goodUUID = tstWorker._file_metadata_id

    tstWorker._metadata_id  = "123456789abcdefghijkl"
    assert tstDist.run() == (
        False, "No valid metadata_identifier provided, cannot delete file"
    )

    # Set the identifier and try to delete again, but fail on unlink
    tstDist._metadata_id = goodUUID
    with monkeypatch.context() as mp:
        mp.setattr("os.unlink", causeOSError)
        assert tstDist.run() == (
            False, "Failed to delete file: a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b.xml"
        )

    # Delete properly
    assert tstDist.run() == (
        True, "Deleted file: a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b.xml"
    )

    # Delete again should fail as the file no longer exists
    assert tstDist.run() == (
        False, "File not found: a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b.xml"
    )

# END Test testDistSolR_Delete


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
import logging
import lxml
import pytest
import tempfile

from tools import causeOSError
from tools import causeException
from tools import readFile

from dmci.config import Config
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

    def update_parent(self, *args, **kwargs):
        return True, "Test successful update message"


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


@pytest.mark.dist
def testDistSolR_Init_with_auth(tmpConf, mockXsd, mockXslt, tmpDir, mockXml, monkeypatch):

    import ipdb
    ipdb.set_trace()
    sd = SolRDist("insert", xml_file=mockXml)

    fd.close()
    os.remove(tmpfname)


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
        # Check that it fails correctly if index_record raises an
        # exception
        mp.setattr(MockIndexMMD, "index_record", causeException)
        assert tstDist._add() == (
            False, "Could not index file %s: Test Exception" % mockXml
        )


@pytest.mark.dist
def testDistSolR_add_successful_with_related_dataset(mockXml, monkeypatch):
    """ Test that the _add function successfully completes with the
    correct return message.
    """
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR",
            lambda *args, **kwargs: MockMMD4SolR(*args, **kwargs))
        mp.setattr("dmci.distributors.solr_dist.IndexMMD",
            lambda *args, **kwargs: MockIndexMMD(*args, **kwargs))
        mp.setattr(MockMMD4SolR, "tosolr",
            lambda *a, **k: {
                "doc": None,
                "id": "no-met-dev-250ba38f-1081-4669-a429-f378c569db32",
                "metadata_identifier": "no.met.dev:250ba38f-1081-4669-a429-f378c569db32",
                "related_dataset": "no.met.dev:350ba38f-1081-4669-a429-f378c569db32",
                "related_dataset_id": "no-met-dev-350ba38f-1081-4669-a429-f378c569db32",
            })

        # Initialise object, and check that it validates
        tstDist = SolRDist("insert", xml_file=mockXml)
        assert tstDist.is_valid()
        assert tstDist._add() == (
            True, "test"
        )

        # Check that it fails correctly if index_record raises an
        # exception
        mp.setattr(MockIndexMMD, "index_record", causeException)
        assert tstDist._add() == (
            False, "Could not index file %s: Test Exception" % mockXml
        )

        # And failing when it fails to update_parent
        mp.setattr(MockIndexMMD, "update_parent", lambda *a, **k: (False, "No parent"))
        assert tstDist._add() == (
            False, "No parent"
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


@pytest.mark.dist
def testDistSolR_add_doc_exists(mockXml, monkeypatch):
    """ Test that an the _add function fails correctly when the
    dataset already exists.
    """
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.IndexMMD.get_dataset",
            lambda *a, **k: {"doc": "test"})
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR.check_mmd", lambda *a, **k: None)
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR.tosolr",
            lambda *a, **k: {
                'id': 'no-met-dev-250ba38f-1081-4669-a429-f378c569db32',
                'metadata_identifier': 'no.met.dev:250ba38f-1081-4669-a429-f378c569db32',
            })
        tstDist = SolRDist("insert", xml_file=mockXml)
        assert tstDist._add() == (
            False,
            "Document already exists in index, no.met.dev:250ba38f-1081-4669-a429-f378c569db32"
        )


@pytest.mark.dist
def testDistSolR_Delete(tmpUUID, tmpDir, filesDir, monkeypatch):

    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.IndexMMD.delete",
            lambda *a, **k: (False, "test failed delete"))
        obj = SolRDist("delete", metadata_id=tmpUUID)
        assert obj._delete() == ( # dette skulle ikke gått uten namespac
            False, "test failed delete"
        )

    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.IndexMMD.delete",
            lambda *a, **k: (True, "test succeed delete"))
        obj = SolRDist("delete", metadata_id=tmpUUID)
        assert obj._delete() == ( # dette skulle ikke gått uten namespac
            True, "test succeed delete"
        )

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
import pytest
import uuid
from tools import causeException

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
            'id': 'no-test-250ba38f-1081-4669-a429-f378c569db32',
            'metadata_identifier': 'no.test:250ba38f-1081-4669-a429-f378c569db32',
        }
        return solr_formatted


class MockFailIndexMMD:
    def __init__(self, *args, **kwargs):
        pass

    def get_dataset(self, *args, **kwargs):
        is_indexed = None
        return is_indexed

    def index_record(self, *args, **kwargs):
        return True, 'test'

    def update_parent(self, *args, **kwargs):
        return True, "Test successful update message"


class mockResp:
    text = "Mock response"
    status_code = 200


class mockWorker:
    _namespace = ""


@pytest.mark.dist
def testDistSolR_Init(tmpUUID):
    """Test the SolRDist class init."""
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert SolRDist("insert", metadata_UUID=tmpUUID).is_valid() is False
    assert SolRDist("update", metadata_UUID=tmpUUID).is_valid() is False
    assert SolRDist("delete", metadata_UUID=tmpUUID).is_valid() is True
    assert SolRDist("blabla", metadata_UUID=tmpUUID).is_valid() is False


@pytest.mark.dist
def testDistSolR_InitAuthentication(mockXml):
    """Test the authentication initiation by
    first initiate a SolRDist without authentication, then call
    _init_authentication with new conf values
    """
    sd = SolRDist("insert", xml_file=mockXml)
    assert sd.is_valid()
    assert sd._conf.solr_username is None
    assert sd._conf.solr_password is None
    assert sd.authentication is None

    sd._conf.solr_username = "username"
    sd._conf.solr_password = "password"
    assert sd._init_authentication() is not None


@pytest.mark.dist
def testDistSolR_Run(mockXml):
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
def testDistSolR_AddMMD4SolRRaisesException(mockXml, monkeypatch):
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
def testDistSolR_AddSuccessful(mockXml, monkeypatch):
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
            False, "Could not index file %s, in SolR. Reason: Test Exception" % mockXml
        )


@pytest.mark.dist
def testDistSolR_AddFailsWithConnectionFailure(mockXml, monkeypatch):
    """ Test that the add function fails when failed to make connection with solr"""
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR",
                   lambda *args, **kwargs: MockMMD4SolR(*args, **kwargs))
        mp.setattr("dmci.distributors.solr_dist.IndexMMD",
                   lambda *args, **kwargs: MockFailIndexMMD(*args, **kwargs))

        # Initialise object, and check that it validates
        tstDist = SolRDist("insert", xml_file=mockXml)
        assert tstDist.is_valid()
        assert tstDist._add() == (False, "Failed to insert dataset in SolR.")


def testDistSolR_AddSuccessfulWithRelatedDataset(mockXml, monkeypatch):
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
            False, "Could not index file %s, in SolR. Reason: Test Exception" % mockXml
        )

        # And failing when it fails to update_parent
        mp.setattr(MockIndexMMD, "update_parent",
                   lambda *a, **k: (False, "No parent"))
        assert tstDist._add() == (
            False, "No parent"
        )


@pytest.mark.dist
def testDistSolR_AddTosolrRaisesException(mockXml, monkeypatch):
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
def testDistSolR_AddDocExists(mockXml, monkeypatch):
    """ Test that an the _add function fails correctly when the
    dataset already exists.
    """
    with monkeypatch.context() as mp:
        mp.setattr("dmci.distributors.solr_dist.IndexMMD.get_dataset",
                   lambda *a, **k: {"doc": "test"})
        mp.setattr("dmci.distributors.solr_dist.MMD4SolR.check_mmd",
                   lambda *a, **k: None)
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
def testDistSolR_Delete(monkeypatch, mockXml):
    """Test the SolRDist class delete via distributor.run()"""
    md_uuid = uuid.UUID("250ba38f-1081-4669-a429-f378c569db32")

    assert SolRDist("delete").run() == (False, "The run job is invalid")
    assert SolRDist("delete", xml_file=mockXml).run() == (False, "The run job is invalid")

    assert SolRDist("delete", metadata_UUID=md_uuid, worker=mockWorker).is_valid()

    # mysolr.delete with and without namespace
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.solr_dist.IndexMMD.delete", lambda self, my_id, *b, **k:
            ("Mock Response", my_id)
        )
        with pytest.raises(ValueError):
            res = SolRDist("delete", metadata_UUID=md_uuid, worker=mockWorker).run()

        mockWorker._namespace = "no.test"
        res = SolRDist("delete", metadata_UUID=md_uuid, worker=mockWorker).run()
        assert res == ("Mock Response", "no.test:250ba38f-1081-4669-a429-f378c569db32")

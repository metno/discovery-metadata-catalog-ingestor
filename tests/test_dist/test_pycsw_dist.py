# -*- coding: utf-8 -*-
"""
DMCI : PyCSW Distributor Class Test
===================================

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
import requests

from tools import writeFile

from dmci.distributors.pycsw_dist import PyCSWDist

# Fixtures
# ========

@pytest.fixture(scope="module")
def distDir(tmpDir):
    distDir = os.path.join(tmpDir, "dist")
    if not os.path.isdir(distDir):
        os.mkdir(distDir)
    return distDir

@pytest.fixture(scope="module")
def dummyXml(distDir):
    dummyXml = os.path.join(distDir, "dummy.xml")
    writeFile(dummyXml, "<xml />")
    return dummyXml

@pytest.fixture(scope="module")
def dummyXslt(distDir):
    dummyXslt = os.path.join(distDir, "dummy.xslt")
    writeFile(dummyXslt, "<xml />")
    return dummyXslt

# Tests
# =====

@pytest.mark.dist
def testDistPyCSW_Init():
    """Test the PyCSWDist class init.
    """
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert PyCSWDist("insert", metadata_id="some_id").is_valid() is True # Should be false...
    assert PyCSWDist("update", metadata_id="some_id").is_valid() is True # should be false
    assert PyCSWDist("delete", metadata_id="some_id").is_valid() is True
    assert PyCSWDist("blabla", metadata_id="some_id").is_valid() is False

# END Test testDistPyCSW_Init

@pytest.mark.dist
def testDistPyCSW_Run():
    # WRONG command test
    assert PyCSWDist("blabla", metadata_id="some_id").run() is False
    assert PyCSWDist("insert", metadata_id="some_id").run() is False

@pytest.mark.dist
def testDistPyCSW_Insert(monkeypatch, dummyXml, dummyXslt):
    """_insert tests
    """
    # return false when xml_file is None
    assert PyCSWDist("insert")._insert(dummyXslt) is False

    # insert returns True
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: "<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: "a response object"
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        assert PyCSWDist("insert", xml_file=dummyXml)._insert(dummyXslt) is True

    # insert returns false
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: "<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: "a response object"
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        assert PyCSWDist("insert", xml_file=dummyXml)._insert(dummyXslt) is False

# END Test testDistPyCSW_Insert

@pytest.mark.dist
def testDistPyCSW_Update(dummyXml):
    """_update tests
    """
    assert PyCSWDist("update", xml_file=dummyXml).run() is False
    assert PyCSWDist("update", metadata_id="some_id").run() is False

# END Test testDistPyCSW_Update

@pytest.mark.dist
def testDistPyCSW_Delete(monkeypatch, dummyXml):
    """_delete tests
    """
    assert PyCSWDist("delete").run() is False
    assert PyCSWDist("delete", xml_file=dummyXml).run() is False

    # delete returns True
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: "a response object"
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        assert PyCSWDist("delete", metadata_id="some_id").run() is True

    # delete returns false
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: "a response object"
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        assert PyCSWDist("delete", metadata_id="some_id").run() is False

    # return false if metadata id is None
    assert PyCSWDist("delete")._delete() is False

# END delete tests

@pytest.mark.dist
def testDistPyCSW_Translate(monkeypatch, dummyXml, dummyXslt):
    """_translate tests
    """
    monkeypatch.setattr(
        "dmci.distributors.pycsw_dist.xml_translate", lambda *a: "some xml code"
    )
    assert PyCSWDist("insert", xml_file=dummyXml)._translate(dummyXslt) == "some xml code"

# END Test testDistPyCSW_Translate

@pytest.mark.dist
def testDistPyCSW_GetTransactionStatus(monkeypatch, dummyXml):
    """_get_transaction_status tests
    """
    # wrong key
    resp = requests.models.Response()
    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status("tull", resp) is False

    # invalud response object
    resp = "hei"
    assert PyCSWDist(
        "insert", xml_file=dummyXml
    )._get_transaction_status(
        "total_inserted", resp
    ) is False
    # Returns true/false

    requests.models.Response.text = property(lambda x: "kjkjh")
    resp = requests.models.Response()
    monkeypatch.setattr(resp, "status_code", 200)

    with monkeypatch.context() as mp:
        # Should return False because _read_response_text returns False
        mp.setattr(PyCSWDist, "_read_response_text", lambda *a, **k: True)
        assert PyCSWDist(
            "insert", xml_file=dummyXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is True

    with monkeypatch.context() as mp:
        # Should return False because of status 300:
        mp.setattr(PyCSWDist, "_read_response_text", lambda *a, **k: False)
        assert PyCSWDist(
            "insert", xml_file=dummyXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is False

    with monkeypatch.context() as mp:
        mp.setattr(resp, "status_code", 300)
        mp.setattr(PyCSWDist, "_read_response_text", lambda *a, **k: True)
        assert PyCSWDist(
            "insert", xml_file=dummyXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is False

# END Test testDistPyCSW_GetTransactionStatus

@pytest.mark.dist
def testDistPyCSW_ReadResponse(dummyXml):
    """_read_response_text tests
    """
    # text wrongkey
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text("tull", "some text") is False

    # text insert succeeds
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dct="http://purl.org/dc/terms/" '
        'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
        'xmlns:gml="http://www.opengis.net/gml" '
        'xmlns:ows="http://www.opengis.net/ows" '
        'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
        'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
        'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
        '<csw:TransactionSummary>'
        '<csw:totalInserted>1</csw:totalInserted>'
        '<csw:totalUpdated>0</csw:totalUpdated>'
        '<csw:totalDeleted>0</csw:totalDeleted>'
        '</csw:TransactionSummary>'
        '<csw:InsertResult>'
        '<csw:BriefRecord>'
        '<dc:identifier>'
        'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
        '</dc:identifier>'
        '<dc:title>Date: 2020-04-20T02:32:44.006Z, Instrument: SAR-C SAR, '
        'Mode: HH HV, Satellite: Sentinel-1, Size: 434.58 MB'
        '</dc:title>'
        '</csw:BriefRecord>'
        '</csw:InsertResult>'
        '</csw:TransactionResponse>'
    )
    key = "total_inserted"
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text(key, text) is True

    # insert but dataset already exists
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<ows:ExceptionReport xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dct="http://purl.org/dc/terms/" '
        'xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" '
        'xmlns:ows="http://www.opengis.net/ows" xmlns:xs="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.2.0" '
        'language="en-US" xsi:schemaLocation="http://www.opengis.net/ows '
        'http://schemas.opengis.net/ows/1.0.0/owsExceptionReport.xsd">'
        '<ows:Exception exceptionCode="NoApplicableCode" locator="insert">'
        '<ows:ExceptionText>Transaction (insert) failed: (psycopg2.errors.UniqueViolation) '
        'duplicate key value violates unique constraint "records_pkey"'
        'DETAIL: '
        'Key (identifier)=('
        'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB) '
        'already exists.'
        '</ows:ExceptionText></ows:Exception></ows:ExceptionReport>'
    )
    key = "total_inserted"
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text(key, text) is False

    # successful delete
    md_id = "S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB"
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dct="http://purl.org/dc/terms/" '
        'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
        'xmlns:gml="http://www.opengis.net/gml" xmlns:ows="http://www.opengis.net/ows" '
        'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
        'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
        'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
        '<csw:TransactionSummary>'
        '<csw:totalInserted>0</csw:totalInserted>'
        '<csw:totalUpdated>0</csw:totalUpdated>'
        '<csw:totalDeleted>1</csw:totalDeleted>'
        '</csw:TransactionSummary>'
        '</csw:TransactionResponse>'
    )
    assert PyCSWDist(
        "delete", metadata_id=md_id
    )._read_response_text(
        "total_deleted", text
    ) is True

    # unsuccessful delete
    md_id = "S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB"
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dct="http://purl.org/dc/terms/" '
        'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
        'xmlns:gml="http://www.opengis.net/gml" xmlns:ows="http://www.opengis.net/ows" '
        'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
        'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
        'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
        '<csw:TransactionSummary>'
        '<csw:totalInserted>0</csw:totalInserted>'
        '<csw:totalUpdated>0</csw:totalUpdated>'
        '<csw:totalDeleted>0</csw:totalDeleted>'
        '</csw:TransactionSummary>'
        '</csw:TransactionResponse>'
    )
    assert PyCSWDist(
        "delete", metadata_id=md_id
    )._read_response_text(
        "total_deleted", text
    ) is False

    # delete unknown tag
    md_id = "S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB"
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<SomeNewTagName>'
        '<TransactionSummary>'
        '<totalInserted>0</totalInserted>'
        '<totalUpdated>0</totalUpdated>'
        '<totalDeleted>1</totalDeleted>'
        '</TransactionSummary>'
        '</SomeNewTagName>'
    )
    assert PyCSWDist(
        "delete", metadata_id=md_id
    )._read_response_text(
        "total_deleted", text
    ) is False

# END Test testDistPyCSW__ReadResponse

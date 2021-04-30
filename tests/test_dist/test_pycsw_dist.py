# -*- coding: utf-8 -*-
"""
DMCI : PyCSW Distributor Class Test
=================================

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
import requests

from dmci.distributors.pycsw_dist import PyCSWDist

def transactionResponseInsert():
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
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
            '</csw:TransactionResponse>')
    return text

def exceptionReportInsertAlreadyExists():
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
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
            '</ows:ExceptionText></ows:Exception></ows:ExceptionReport>')
    return text

def transactionResponseDelete():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
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
            '</csw:TransactionResponse>')
    return [id, text]

def transactionResponseDeleteFails():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
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
            '</csw:TransactionResponse>')
    return [id, text]

def transactionResponseDeleteUnknownTagName():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<SomeNewTagName>'
            '<TransactionSummary>'
            '<totalInserted>0</totalInserted>'
            '<totalUpdated>0</totalUpdated>'
            '<totalDeleted>1</totalDeleted>'
            '</TransactionSummary>'
            '</SomeNewTagName>')
    return [id, text]

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

# WRONG command test
@pytest.mark.dist
def testDistPyCSW_Run_blabla():
    assert PyCSWDist("blabla", metadata_id="some_id").run() is False

# END wrong command test

# _insert tests
@pytest.mark.dist
def testDistPyCSW_Run_insert__with__wrong__arg():
    """Test the PyCSWDist class run function.
    """
    assert PyCSWDist("insert", metadata_id="some_id").run() is False

@pytest.mark.dist
def testDistPyCSW__insert_return__false__when__xml_file__is__None(monkeypatch, dummyXslt):
    assert PyCSWDist("insert")._insert(dummyXslt) is False

@pytest.mark.dist
def testDistPyCSW__insert__returns__true(monkeypatch, tmpDir, dummy_xml_str, dummyXml, dummyXslt):
    def mockreturn(*args):
        return dummy_xml_str
    monkeypatch.setattr(PyCSWDist, '_translate', mockreturn)
    monkeypatch.setattr('dmci.distributors.pycsw_dist.requests.post',
                        lambda *args, **kwargs: 'a response object')

    def mock_get_transaction_status(*args):
        return True
    monkeypatch.setattr(PyCSWDist, '_get_transaction_status', mock_get_transaction_status)
    assert PyCSWDist("insert", xml_file=dummyXml)._insert(dummyXslt) is True

@pytest.mark.dist
def testDistPyCSW__insert__returns__false(monkeypatch, dummyXml, dummyXslt, dummy_xml_str):
    def mockreturn(*args):
        return dummy_xml_str
    monkeypatch.setattr(PyCSWDist, '_translate', mockreturn)
    monkeypatch.setattr('dmci.distributors.pycsw_dist.requests.post',
                        lambda *args, **kwargs: 'a response object')

    def mock_get_transaction_status(*args):
        return False
    monkeypatch.setattr(PyCSWDist, '_get_transaction_status', mock_get_transaction_status)
    assert PyCSWDist("insert", xml_file=dummyXml)._insert(dummyXslt) is False

# END insert tests

# _update tests
@pytest.mark.dist
def testDistPyCSW__update(dummyXml):
    assert PyCSWDist("update", xml_file=dummyXml).run() is False

@pytest.mark.dist
def testDistPyCSW_Run_update():
    assert PyCSWDist("update", metadata_id="some_id").run() is False

# END update tests

# _delete tests
@pytest.mark.dist
def testDistPyCSW_Run_delete__fails_if_no_id(dummyXml):
    assert PyCSWDist("delete").run() is False
    assert PyCSWDist("delete", xml_file=dummyXml).run() is False

@pytest.mark.dist
def testDistPyCSW_run_delete__returns_true(monkeypatch):
    monkeypatch.setattr('dmci.distributors.pycsw_dist.requests.post',
                        lambda *args, **kwargs: 'a response object')

    def mock_get_transaction_status(*args):
        return True
    monkeypatch.setattr(PyCSWDist, '_get_transaction_status', mock_get_transaction_status)
    assert PyCSWDist("delete", metadata_id="some_id").run() is True

@pytest.mark.dist
def testDistPyCSW_run_delete__returns_false(monkeypatch):
    monkeypatch.setattr('dmci.distributors.pycsw_dist.requests.post',
                        lambda *args, **kwargs: 'a response object')

    def mock_get_transaction_status(*args):
        return False
    monkeypatch.setattr(PyCSWDist, '_get_transaction_status', mock_get_transaction_status)
    assert PyCSWDist("delete", metadata_id="some_id").run() is False

@pytest.mark.dist
def testDistPyCSW__delete__return__false__if___metadata_id_is_None():
    assert PyCSWDist("delete")._delete() is False

# END delete tests

# _translate tests
@pytest.mark.dist
def testDistPyCSW__translate(monkeypatch, dummyXml, dummyXslt):
    monkeypatch.setattr('dmci.distributors.pycsw_dist.xml_translate',
                        lambda *args: 'some xml code')
    assert PyCSWDist("insert", xml_file=dummyXml)._translate(dummyXslt) == 'some xml code'

# END translate tests

# _get_transaction_status tests
@pytest.mark.dist
def testDistPyCSW__get_transaction_status__wrong_key(monkeypatch, dummyXml):
    resp = requests.models.Response()

    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status('tull', resp) is False

@pytest.mark.dist
def testDistPyCSW__get_transaction_status__invalid_response_obj(monkeypatch, dummyXml):
    resp = 'hei'
    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status('total_inserted',
                                                                          resp) is False

@pytest.mark.dist
def testDistPyCSW__get_transaction_status_returns_true_false(monkeypatch, dummyXml):
    def get_text(self): # called like resp.text, i.e., self
        return 'kjkjh'

    requests.models.Response.text = property(get_text)
    resp = requests.models.Response()
    monkeypatch.setattr(resp, 'status_code', 200)
    # Should return True because _read_response_text returns True

    def mock_read_response_text(*args, **kwargs):
        return True
    monkeypatch.setattr(PyCSWDist, '_read_response_text', mock_read_response_text)
    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status('total_inserted',
                                                                          resp) is True
    # Should return False because _read_response_text returns False

    def mock_read_response_text(*args, **kwargs):
        return False
    monkeypatch.setattr(PyCSWDist, '_read_response_text', mock_read_response_text)
    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status('total_inserted',
                                                                          resp) is False
    # Should return False because of status 300:
    monkeypatch.setattr(resp, 'status_code', 300)

    def mock_read_response_text(*args, **kwargs):
        return True
    monkeypatch.setattr(PyCSWDist, '_read_response_text', mock_read_response_text)
    assert PyCSWDist("insert", xml_file=dummyXml)._get_transaction_status('total_inserted',
                                                                          resp) is False

# END _get_transaction_status tests

# _read_response_text tests

@pytest.mark.dist
def testDistPyCSW__read_response_text__wrong_key(dummyXml):
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text('tull', 'some text') is False

@pytest.mark.dist
def testDistPyCSW__read_response_text__insert_succeeds(dummyXml):
    text = transactionResponseInsert()
    key = 'total_inserted'
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text(key, text) is True

@pytest.mark.dist
def testDistPyCSW__read_response_text__insert_but_dataset_already_exists(dummyXml):
    text = exceptionReportInsertAlreadyExists()
    key = 'total_inserted'
    assert PyCSWDist("insert", xml_file=dummyXml)._read_response_text(key, text) is False

@pytest.mark.dist
def testDistPyCSW__read_response_text__successful_delete():
    id = transactionResponseDelete()[0]
    text = transactionResponseDelete()[1]
    assert PyCSWDist('delete', metadata_id=id)._read_response_text('total_deleted', text) is True

@pytest.mark.dist
def testDistPyCSW__read_response_text__unsuccessful_delete():
    id = transactionResponseDeleteFails()[0]
    text = transactionResponseDeleteFails()[1]
    assert PyCSWDist('delete', metadata_id=id)._read_response_text('total_deleted', text) is False

def testDistPyCSW__read_response_text__delete_but_unknown_tag():
    id = transactionResponseDeleteUnknownTagName()[0]
    text = transactionResponseDeleteUnknownTagName()[1]
    assert PyCSWDist('delete', metadata_id=id)._read_response_text('total_deleted', text) is False

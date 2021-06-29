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

from lxml import etree

from dmci.distributors.pycsw_dist import PyCSWDist


@pytest.mark.dist
def testDistPyCSW_Init():
    """Test the PyCSWDist class init."""
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert PyCSWDist("insert", metadata_id="some_id").is_valid() is False
    assert PyCSWDist("update", metadata_id="some_id").is_valid() is False
    assert PyCSWDist("delete", metadata_id="some_id").is_valid() is True
    assert PyCSWDist("blabla", metadata_id="some_id").is_valid() is False

# END Test testDistPyCSW_Init


@pytest.mark.dist
def testDistPyCSW_Run():
    """Test the run command with invalid parameters."""
    # WRONG command test
    assert PyCSWDist("blabla", metadata_id="some_id").run() == (False, "The run job is invalid")
    assert PyCSWDist("insert", metadata_id="some_id").run() == (False, "The run job is invalid")

# END Test testDistPyCSW_Run


@pytest.mark.dist
def testDistPyCSW_Insert(monkeypatch, mockXml, mockXslt):
    """Test insert commands via run()."""
    class mockResp:
        text = "Mock response"
        status_code = 200

    # insert returns True
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        tstPyCSW = PyCSWDist("insert", xml_file=mockXml)
        tstPyCSW._conf.mmd_xslt_path = mockXslt
        assert tstPyCSW.run() == (True, "Mock response")

    # insert returns false
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        tstPyCSW = PyCSWDist("insert", xml_file=mockXml)
        tstPyCSW._conf.mmd_xslt_path = mockXslt
        assert tstPyCSW.run() == (False, "Mock response")

# END Test testDistPyCSW_Insert


@pytest.mark.dist
def testDistPyCSW_Update(mockXml):
    """Test update commands via run()."""
    assert PyCSWDist("update", xml_file=mockXml).run() == (False, "Not yet implemented")
    assert PyCSWDist("update", metadata_id="some_id").run() == (False, "The run job is invalid")

# END Test testDistPyCSW_Update


@pytest.mark.dist
def testDistPyCSW_Delete(monkeypatch, mockXml):
    """Test delete commands via run()."""
    class mockResp:
        text = "Mock response"
        status_code = 200

    assert PyCSWDist("delete").run() == (False, "The run job is invalid")
    assert PyCSWDist("delete", xml_file=mockXml).run() == (False, "The run job is invalid")

    # delete returns True
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        assert PyCSWDist("delete", metadata_id="some_id").run() == (True, "Mock response")

    # delete returns false
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        assert PyCSWDist("delete", metadata_id="some_id").run() == (False, "Mock response")

# END Test testDistPyCSW_Delete


@pytest.mark.dist
def testDistPyCSW_Translate(filesDir, caplog):
    """_translate tests"""
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "not_an_xml_file.xml")
    xsltFile = os.path.join(filesDir, "mmd", "mmd-to-geonorge.xslt")

    outFile = os.path.join(filesDir, "reference", "pycsw_dist_translated.xml")
    outTree = etree.parse(outFile, parser=etree.XMLParser(remove_blank_text=True))
    outData = etree.tostring(outTree, pretty_print=False, encoding="utf-8")

    tstPyCSW = PyCSWDist("insert", xml_file=passFile)
    tstPyCSW._conf.mmd_xslt_path = xsltFile
    result = tstPyCSW._translate()
    assert isinstance(result, bytes)
    assert result == outData

    caplog.clear()
    tstPyCSW = PyCSWDist("insert", xml_file=failFile)
    tstPyCSW._conf.mmd_xslt_path = xsltFile
    assert tstPyCSW._translate() == b""
    assert "Failed to translate MMD to ISO19139" in caplog.messages

# END Test testDistPyCSW_Translate


@pytest.mark.dist
def testDistPyCSW_GetTransactionStatus(monkeypatch, mockXml):
    """_get_transaction_status tests"""
    # wrong key
    resp = requests.models.Response()
    assert PyCSWDist("insert", xml_file=mockXml)._get_transaction_status("tull", resp) is False

    # invalud response object
    resp = "hei"
    assert PyCSWDist(
        "insert", xml_file=mockXml
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
            "insert", xml_file=mockXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is True

    with monkeypatch.context() as mp:
        # Should return False because of status 300:
        mp.setattr(PyCSWDist, "_read_response_text", lambda *a, **k: False)
        assert PyCSWDist(
            "insert", xml_file=mockXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is False

    with monkeypatch.context() as mp:
        mp.setattr(resp, "status_code", 300)
        mp.setattr(PyCSWDist, "_read_response_text", lambda *a, **k: True)
        assert PyCSWDist(
            "insert", xml_file=mockXml
        )._get_transaction_status(
            "total_inserted", resp
        ) is False

# END Test testDistPyCSW_GetTransactionStatus


@pytest.mark.dist
def testDistPyCSW_ReadResponse(mockXml, caplog):
    """_read_response_text tests"""
    # text wrongkey
    assert PyCSWDist("insert", xml_file=mockXml)._read_response_text("tull", "some text") is False

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
    assert PyCSWDist("insert", xml_file=mockXml)._read_response_text(key, text) is True

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
    assert PyCSWDist("insert", xml_file=mockXml)._read_response_text(key, text) is False

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
    caplog.clear()
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
    assert "This should not happen" in caplog.messages

    # insert but dataset already exists, unparsable error
    caplog.clear()
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
        '</ows:ExceptionReport>'
    )
    key = "total_inserted"
    assert PyCSWDist("insert", xml_file=mockXml)._read_response_text(key, text) is False
    assert "Unknown Error" in caplog.messages

    # unparsable response (truncated)
    caplog.clear()
    text = (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
        '<!-- pycsw 2.7.dev0 -->'
        '<ows:ExceptionReport xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dct="http://purl.org/dc/terms/" '
        'xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" '
        'xmlns:ows="http:/'
    )
    key = "total_inserted"
    assert PyCSWDist("insert", xml_file=mockXml)._read_response_text(key, text) is False
    assert "Could not parse response XML from PyCSW" in caplog.messages

# END Test testDistPyCSW_ReadResponse

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
from tools import causeException

from lxml import etree

from dmci.api.worker import Worker
from dmci.distributors.pycsw_dist import PyCSWDist


class mockResp:
    text = "Mock response"
    status_code = 200


class mockWorker:
    _namespace = ""


@pytest.mark.dist
def testDistPyCSW_Init(tmpUUID):
    """Test the PyCSWDist class init."""
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert PyCSWDist("insert", metadata_UUID=tmpUUID).is_valid() is False
    assert PyCSWDist("update", metadata_UUID=tmpUUID).is_valid() is False
    assert PyCSWDist("delete", metadata_UUID=tmpUUID).is_valid() is True
    assert PyCSWDist("blabla", metadata_UUID=tmpUUID).is_valid() is False

# END Test testDistPyCSW_Init


@pytest.mark.dist
def testDistPyCSW_Run(tmpUUID):
    """Test the run command with invalid parameters."""
    # WRONG command test
    assert PyCSWDist("blabla", metadata_UUID=tmpUUID).run() == (False, "The run job is invalid")
    assert PyCSWDist("insert", metadata_UUID=tmpUUID).run() == (False, "The run job is invalid")

# END Test testDistPyCSW_Run


@pytest.mark.dist
def testDistPyCSW_Insert(monkeypatch, mockXml, mockXslt):
    """Test insert commands via run()."""
    # Insert returns True
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        tstPyCSW = PyCSWDist("insert", xml_file=mockXml)
        tstPyCSW._conf.mmd_xsl_path = mockXslt
        assert tstPyCSW.run() == (True, "Mock response")

    # Insert returns False
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)

        tstPyCSW = PyCSWDist("insert", xml_file=mockXml)
        tstPyCSW._conf.mmd_xsl_path = mockXslt
        assert tstPyCSW.run() == (False, "Mock response")

    # Insert returns False if the http post request fails
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", causeException)
        tstPyCSW = PyCSWDist("insert", xml_file=mockXml)
        assert tstPyCSW.run() == (
            False,
            "http://localhost: service unavailable. Failed to insert."
        )

    # END Test testDistPyCSW_Insert


@pytest.mark.dist
def testDistPyCSW_Update(monkeypatch, mockXml, mockXslt, tmpUUID):
    """Test update commands via run()."""

    tstWorker = Worker("update", None, None)
    tstWorker._file_metadata_id = tmpUUID

    # Update returns True
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        tstPyCSW = PyCSWDist("update", xml_file=mockXml)
        tstPyCSW._worker = tstWorker
        tstPyCSW._conf.mmd_xsl_path = mockXslt
        assert tstPyCSW.run() == (True, "Mock response")
        tstPyCSW._worker._namespace = "test.no"
        assert tstPyCSW.run() == (True, "Mock response")

    # Update returns False
    with monkeypatch.context() as mp:
        mp.setattr(PyCSWDist, "_translate", lambda *a: b"<xml />")
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        tstPyCSW = PyCSWDist("update", xml_file=mockXml)
        tstPyCSW._worker = tstWorker
        tstPyCSW._conf.mmd_xsl_path = mockXslt
        assert tstPyCSW.run() == (False, "Mock response")

    # Update returns False if the http post request fails
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", causeException)
        tstPyCSW = PyCSWDist("update", xml_file=mockXml)
        assert tstPyCSW.run() == (
            False,
            "http://localhost: service unavailable. Failed to update."
        )

# END Test testDistPyCSW_Update


@pytest.mark.dist
def testDistPyCSW_Delete(monkeypatch, mockXml, tmpUUID):
    """Test delete commands via run()."""

    assert PyCSWDist("delete").run() == (False, "The run job is invalid")
    assert PyCSWDist("delete", xml_file=mockXml).run() == (False, "The run job is invalid")

    # delete returns True
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: True)
        with pytest.raises(ValueError):
            res = PyCSWDist("delete", metadata_UUID=tmpUUID, worker=mockWorker).run()

        mockWorker._namespace = "test.no"
        res = PyCSWDist("delete", metadata_UUID=tmpUUID, worker=mockWorker).run()
        assert res == (True, "Mock response")

    # delete returns false
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", lambda *a, **k: mockResp
        )
        mp.setattr(PyCSWDist, "_get_transaction_status", lambda *a: False)
        mockWorker._namespace = "test.no"
        res = PyCSWDist("delete", metadata_UUID=tmpUUID, worker=mockWorker).run()
        assert res == (False, "Mock response")

    # Delete returns False if http post request fails
    with monkeypatch.context() as mp:
        mp.setattr(
            "dmci.distributors.pycsw_dist.requests.post", causeException)
        res = PyCSWDist("delete", metadata_UUID=tmpUUID, worker=mockWorker).run()
        assert res == (False, "http://localhost: service unavailable. Failed to delete.")

# END Test testDistPyCSW_Delete


@pytest.mark.dist
def testDistPyCSW_Translate(filesDir, caplog):
    """_translate tests"""
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "not_an_xml_file.xml")
    xslFile = os.path.join(filesDir, "mmd", "mmd-to-geonorge.xsl")
    path_to_parent_list = os.path.join(filesDir, "mmd", "parent-uuid-list.xml")

    outFile = os.path.join(filesDir, "reference", "pycsw_dist_translated.xml")
    outTree = etree.parse(outFile, parser=etree.XMLParser(remove_blank_text=True))
    outData = etree.tostring(outTree, pretty_print=False, encoding="utf-8")

    tstPyCSW = PyCSWDist("insert", xml_file=passFile, path_to_parent_list=path_to_parent_list)
    tstPyCSW._conf.mmd_xsl_path = xslFile
    result = tstPyCSW._translate()
    assert isinstance(result, bytes)
    assert result == outData

    caplog.clear()
    tstPyCSW = PyCSWDist("insert", xml_file=failFile)
    tstPyCSW._conf.mmd_xsl_path = xslFile
    assert tstPyCSW._translate() == b""
    assert "Failed to translate MMD to ISO19139" in caplog.messages

# END Test testDistPyCSW_Translate


@pytest.mark.dist
def testDistPyCSW_Translate_Parent(filesDir):
    """_translate tests"""
    passFile = os.path.join(filesDir, "api", "aqua-modis-parent.xml")
    xslFile = os.path.join(filesDir, "mmd", "mmd-to-geonorge.xsl")
    path_to_parent_list = os.path.join(filesDir, "mmd", "parent-uuid-list.xml")

    outFile = os.path.join(filesDir, "reference", "parent_translated.xml")
    outTree = etree.parse(outFile, parser=etree.XMLParser(remove_blank_text=True))
    outData = etree.tostring(outTree, pretty_print=False, encoding="utf-8")
    # THis is ugly, but when reading the xml file content, \xc3\xb8 becomes the representation
    # \\xc3\\xb8.
    outData = outData.replace(b'\\xc3\\xb8', b'\xc3\xb8')
    tstPyCSW = PyCSWDist("insert", xml_file=passFile, path_to_parent_list=path_to_parent_list)
    tstPyCSW._conf.mmd_xsl_path = xslFile
    result = tstPyCSW._translate()

    assert isinstance(result, bytes)
    assert result == outData

# END Test testDistPyCSW_Translate_Parent


@pytest.mark.dist
def testDistPyCSW_GetTransactionStatus(monkeypatch, mockXml, caplog):
    """_get_transaction_status tests"""
    # wrong key
    resp = requests.models.Response()
    caplog.clear()
    assert PyCSWDist("insert", xml_file=mockXml)._get_transaction_status("tull", resp) is False
    assert "Input key must be one of: total_deleted, total_inserted, total_updated" in caplog.text

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
        "delete", metadata_UUID=md_id
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
        "delete", metadata_UUID=md_id
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
        "delete", metadata_UUID=md_id
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

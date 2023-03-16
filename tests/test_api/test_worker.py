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

from uuid import UUID
from tools import readFile

from dmci.api.worker import Worker
from dmci.distributors import FileDist, PyCSWDist
from dmci.tools import CheckMMD


@pytest.mark.api
def testApiWorker_Init():
    """Test the Worker class init."""
    assert Worker(None, None, None)._dist_cmd is None
    assert Worker("insert", None, None)._dist_cmd == "insert"
    assert Worker("update", None, None)._dist_cmd == "update"
    assert Worker("delete", None, None)._dist_cmd == "delete"
    assert Worker("blabla", None, None)._dist_cmd is None
    assert Worker(None, None, None, a=1)._kwargs == {"a": 1}

# END Test testApiWorker_Init


@pytest.mark.api
def testApiWorker_Distributor(tmpConf, mockXml, monkeypatch):
    """Test the Worker class distributor."""
    tmpConf.call_distributors = ["file", "pycsw", "blabla"]

    # Call the distributor function with the distributors from the config
    with monkeypatch.context() as mp:
        mp.setattr(FileDist, "run", lambda *a: (True, "ok"))
        mp.setattr(PyCSWDist, "run", lambda *a: (True, "ok"))

        tstWorker = Worker("insert", None, None)
        tstWorker._conf = tmpConf
        tstWorker._dist_xml_file = mockXml

        status, valid, called, failed, skipped, failed_msg = tstWorker.distribute()
        assert status is True
        assert valid is True
        assert called == ["file", "pycsw"]
        assert failed == []
        assert skipped == ["blabla"]
        assert failed_msg == []

    # Same as above, but jobs fail
    with monkeypatch.context() as mp:
        mp.setattr(FileDist, "run", lambda *a: (False, "oops"))
        mp.setattr(PyCSWDist, "run", lambda *a: (False, "oops"))

        tstWorker = Worker("insert", None, None)
        tstWorker._conf = tmpConf
        tstWorker._dist_xml_file = mockXml

        status, valid, called, failed, skipped, failed_msg = tstWorker.distribute()
        assert status is False
        assert valid is True
        assert called == []
        assert failed == ["file", "pycsw"]
        assert skipped == ["blabla"]
        assert failed_msg == ["oops", "oops"]

    # Call the distributor function with the wrong parameters
    tstWorker = Worker("insert", None, None)
    tstWorker._conf = tmpConf
    tstWorker._dist_cmd = "blabla"
    tstWorker._dist_xml_file = "/path/to/nowhere"

    status, valid, called, failed, skipped, failed_msg = tstWorker.distribute()
    assert status is True  # No jobs were run since all were skipped
    assert valid is False  # All jobs were invalid due to the command
    assert called == []
    assert failed == []
    assert skipped == ["file", "pycsw", "blabla"]
    assert failed_msg == []

# END Test testApiWorker_Distributor


@pytest.mark.api
def testApiWorker_Validator(monkeypatch, filesDir):
    """Test the Worker class validator."""
    xsdFile = os.path.join(filesDir, "mmd", "mmd.xsd")
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "api", "failing.xml")

    xsdObj = lxml.etree.XMLSchema(lxml.etree.parse(xsdFile))
    passWorker = Worker("none", passFile, xsdObj)
    failWorker = Worker("none", failFile, xsdObj)

    # Invalid data format
    passData = readFile(passFile)
    assert passWorker.validate(passData) == (False, "Input must be bytes type")

    # Valid data format
    with monkeypatch.context() as mp:
        mp.setattr(Worker, "_check_information_content", lambda *a: (True, ""))

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
    """Test _check_information_content."""
    passFile = os.path.join(filesDir, "api", "passing.xml")
    tstWorker = Worker("none", passFile, None)

    # Invalid data format
    passData = readFile(passFile)
    assert tstWorker._check_information_content(passData) == (False, "Input must be bytes type")
    assert tstWorker._file_metadata_id is None

    # Valid data format
    with monkeypatch.context() as mp:
        mp.setattr(CheckMMD, "check_url", lambda *a, **k: (True, []))
        passData = bytes(readFile(passFile), "utf-8")
        assert tstWorker._check_information_content(passData) == (
            True, "Input MMD XML file is ok"
        )
        assert isinstance(tstWorker._file_metadata_id, UUID)

    # Valid data format, invalid content
    with monkeypatch.context() as mp:
        mp.setattr(CheckMMD, "check_url", lambda *a, **k: (False, []))
        passData = bytes(readFile(passFile), "utf-8")
        assert tstWorker._check_information_content(passData) == (
            False, "Input MMD XML file contains errors, please check your file"
        )

    # Invalid XML file
    failFile = os.path.join(filesDir, "api", "failing.xml")
    failData = bytes(readFile(failFile), "utf-8")
    assert tstWorker._check_information_content(failData) == (
        False, "Input MMD XML file has no valid uri:UUID metadata_identifier"
    )

    # Check Error report
    failFile = os.path.join(filesDir, "api", "failing.xml")
    failData = (
        b"<root>"
        b"  <metadata_identifier>met.no:00000000-0000-0000-0000-000000000000</metadata_identifier>"
        b"  <resource>imap://met.no</resource>"
        b"  <geographic_extent>"
        b"    <rectangle>"
        b"      <north>76.199661</north>"
        b"      <south>71.63427</south>"
        b"      <west>-28.114723</west>"
        b"    </rectangle>"
        b"  </geographic_extent>"
        b"</root>"
    )
    assert tstWorker._check_information_content(failData) == (
        False, (
            "Input MMD XML file contains errors, please check your file\n"
            "Failed: URL Check on 'imap://met.no'\n"
            " - URL scheme 'imap' not allowed.\n"
            " - URL contains no path. At least '/' is required.\n"
            "Failed: Rectangle Check\n"
            " - Missing rectangle element 'east'.\n"
        ).rstrip()
    )

# END Test testApiWorker_CheckInfoContent


@pytest.mark.api
def testApiWorker_ExtractMetaDataID(filesDir, mockXml):
    """Test _exctract_metadata_id."""
    passFile = os.path.join(filesDir, "api", "passing.xml")
    failFile = os.path.join(filesDir, "api", "failing.xml")

    no_namespaced_UUID = (
        '<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd" xmlns:gml="http://www.opengis.net/gml">'
        '  <mmd:metadata_identifier>%s</mmd:metadata_identifier>'
        '</mmd:mmd>'
    )%("a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b")

    # ":a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b".split(":") returns two strings:
    # "" and "a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b"
    # This test invokes empty return for namespace
    namespaced_UUID_empty_return = (
        '<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd" xmlns:gml="http://www.opengis.net/gml">'
        '  <mmd:metadata_identifier>%s</mmd:metadata_identifier>'
        '</mmd:mmd>'
    )%(":a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b")

    # format "test:no:a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b" is wrong (should be "test.no:UUID")
    namespaced_UUID_too_many = (
        '<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd" xmlns:gml="http://www.opengis.net/gml">'
        '  <mmd:metadata_identifier>%s</mmd:metadata_identifier>'
        '</mmd:mmd>'
    )%("test:no:a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b")

    # Valid File with Namespace
    passXML = lxml.etree.fromstring(bytes(readFile(passFile), "utf-8"))
    tstWorker = Worker("insert", passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is True
    assert tstWorker._namespace == "test.no"
    assert tstWorker._file_metadata_id is not None

    # Valid mmd with no namespace
    passXML = lxml.etree.fromstring(bytes(no_namespaced_UUID, "utf-8"))
    tstWorker = Worker("insert", passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is False
    assert tstWorker._file_metadata_id is None
    assert tstWorker._namespace is None

    passXML = lxml.etree.fromstring(bytes(namespaced_UUID_empty_return, "utf-8"))
    tstWorker = Worker("insert", passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is False
    assert tstWorker._file_metadata_id is None
    assert tstWorker._namespace is None

    # MMD with invalid namespace
    passXML = lxml.etree.fromstring(bytes(namespaced_UUID_too_many, "utf-8"))
    tstWorker = Worker("insert", passFile, None)
    assert tstWorker._extract_metadata_id(passXML) is False
    assert tstWorker._file_metadata_id is None
    assert tstWorker._namespace is None

    # Invalid File
    mockData = lxml.etree.fromstring(bytes(readFile(mockXml), "utf-8"))
    tstWorker = Worker("insert", mockXml, None)
    assert tstWorker._extract_metadata_id(mockData) is False
    assert tstWorker._file_metadata_id is None
    assert tstWorker._namespace is None

    # Invalid UUID
    failXML = lxml.etree.fromstring(bytes(readFile(failFile), "utf-8"))
    tstWorker = Worker("insert", failFile, None)
    assert tstWorker._extract_metadata_id(failXML) is False
    assert tstWorker._file_metadata_id is None
    assert tstWorker._namespace is None

# END Test testApiWorker_ExtractMetaDataID

@pytest.mark.api
def testApiWorker_AddLandingPage(filesDir):
    passFile = os.path.join(filesDir, "api", "passing.xml")
    passFilewithLP = os.path.join(filesDir, "api", "passing_wlandingpage.xml")
    passFilewithRInoLP = os.path.join(filesDir, "api", "passing_wrelatedinfo_nolandingpage.xml")
    
    catalog_url="https://data.met.no/dataset"
    
    data_wo_landingpage = bytes(readFile(passFile), "utf-8")
    data_w_old_landingpage = bytes(readFile(passFilewithLP), "utf-8")
    data_w_relinf_nolandingpage = bytes(readFile(passFilewithRInoLP), "utf-8")
   
    data_w_landingpage=b'<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd" xmlns:gml="http://www.opengis.net/gml">\n  <mmd:metadata_identifier>test.no:a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b</mmd:metadata_identifier>\n  <mmd:title xml:lang="en">Direct Broadcast data processed in satellite swath to L1C</mmd:title>\n  <mmd:title xml:lang="nor">Direktesendte satellittdata prosessert i satellittsveip til L1C.</mmd:title>\n  <mmd:abstract xml:lang="en">Direct Broadcast data received at MET NORWAY Oslo. Processed by standard processing software to geolocated and calibrated values in satellite swath in received instrument resolution.</mmd:abstract>\n  <mmd:abstract xml:lang="nor">Direktesendte satellittdata mottatt ved Meteorologisk Institutt Oslo. Prosessert med standard prosesseringssoftware til geolokaliserte og kalibrerte verdier i satellitsveip i mottatt instrument oppl\xc3\xb8sning.</mmd:abstract>\n  <mmd:metadata_status>Active</mmd:metadata_status>\n  <mmd:dataset_production_status>Complete</mmd:dataset_production_status>\n  <mmd:collection>METNCS</mmd:collection>\n  <mmd:last_metadata_update>\n    <mmd:update>\n      <mmd:datetime>2021-04-29T00:46:05Z</mmd:datetime>\n      <mmd:type>Created</mmd:type>\n    </mmd:update>\n  </mmd:last_metadata_update>\n  <mmd:temporal_extent>\n    <mmd:start_date>2021-04-29T00:28:44.977627Z</mmd:start_date>\n    <mmd:end_date>2021-04-29T00:39:55.000000Z</mmd:end_date>\n  </mmd:temporal_extent>\n  <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>\n  <mmd:iso_topic_category>environment</mmd:iso_topic_category>\n  <mmd:iso_topic_category>oceans</mmd:iso_topic_category>\n  <mmd:keywords vocabulary="GCMD">\n    <mmd:keyword>Earth Science &gt; Atmosphere &gt; Atmospheric radiation</mmd:keyword>\n    <mmd:resource>https://gcmdservices.gsfc.nasa.gov/static/kms/</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:keywords vocabulary="GEMET">\n    <mmd:keyword>Meteorological geographical features</mmd:keyword>\n    <mmd:keyword>Atmospheric conditions</mmd:keyword>\n    <mmd:keyword>Oceanographic geographical features</mmd:keyword>\n    <mmd:resource>http://inspire.ec.europa.eu/theme</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:keywords vocabulary="Norwegian thematic categories">\n    <mmd:keyword>Weather and climate</mmd:keyword>\n    <mmd:resource>https://register.geonorge.no/subregister/metadata-kodelister/kartverket/nasjonal-temainndeling</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:geographic_extent>\n    <mmd:rectangle srsName="EPSG:4326">\n      <mmd:north>80.49233</mmd:north>\n      <mmd:south>36.540688</mmd:south>\n      <mmd:east>79.40124</mmd:east>\n      <mmd:west>1.5549301</mmd:west>\n    </mmd:rectangle>\n  </mmd:geographic_extent>\n  <mmd:dataset_language>en</mmd:dataset_language>\n  <mmd:operational_status>Operational</mmd:operational_status>\n  <mmd:use_constraint>\n    <mmd:identifier>CC-BY-4.0</mmd:identifier>\n    <mmd:resource>http://spdx.org/licenses/CC-BY-4.0</mmd:resource>\n  </mmd:use_constraint>\n  <mmd:personnel>\n    <mmd:role>Technical contact</mmd:role>\n    <mmd:name>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:name>\n    <mmd:email>post@met.no</mmd:email>\n    <mmd:organisation>MET NORWAY</mmd:organisation>\n  </mmd:personnel>\n  <mmd:personnel>\n    <mmd:role>Metadata author</mmd:role>\n    <mmd:name>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:name>\n    <mmd:email>post@met.no</mmd:email>\n    <mmd:organisation>unknown</mmd:organisation>\n  </mmd:personnel>\n  <mmd:data_center>\n    <mmd:data_center_name>\n      <mmd:short_name>MET NORWAY</mmd:short_name>\n      <mmd:long_name>MET NORWAY</mmd:long_name>\n    </mmd:data_center_name>\n    <mmd:data_center_url>met.no</mmd:data_center_url>\n  </mmd:data_center>\n  <mmd:data_access>\n    <mmd:type>OPeNDAP</mmd:type>\n    <mmd:description>Open-source Project for a Network Data Access Protocol</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/dodsC/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:resource>\n  </mmd:data_access>\n  <mmd:data_access>\n    <mmd:type>OGC WMS</mmd:type>\n    <mmd:description>OGC Web Mapping Service, URI to GetCapabilities Document.</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/wms/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc?service=WMS&amp;version=1.3.0&amp;request=GetCapabilities</mmd:resource>\n    <mmd:wms_layers>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n    </mmd:wms_layers>\n  </mmd:data_access>\n  <mmd:data_access>\n    <mmd:type>HTTP</mmd:type>\n    <mmd:description>Direct download of file</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/fileServer/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:resource>\n  </mmd:data_access>\n  <mmd:related_dataset relation_type="parent">test.no:64db6102-14ce-41e9-b93b-61dbb2cb8b4e</mmd:related_dataset>\n  <mmd:storage_information>\n    <mmd:file_name>aqua-modis-1km-20210429002844-20210429003955.nc</mmd:file_name>\n    <mmd:file_location>/lustre/storeB/immutable/archive/projects/remotesensing/satellite-thredds/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:file_location>\n    <mmd:file_format>NetCDF-CF</mmd:file_format>\n    <mmd:file_size unit="MB">1862.00</mmd:file_size>\n    <mmd:checksum type="md5sum">4e1833610272ee63228f575d1c875fbe</mmd:checksum>\n  </mmd:storage_information>\n  <mmd:project>\n    <mmd:short_name>Govermental core service</mmd:short_name>\n    <mmd:long_name>Govermental core service</mmd:long_name>\n  </mmd:project>\n  <mmd:platform>\n    <mmd:short_name>Aqua</mmd:short_name>\n    <mmd:long_name>Aqua</mmd:long_name>\n    <mmd:resource>https://www.wmo-sat.info/oscar/satellites/view/aqua</mmd:resource>\n    <mmd:instrument>\n      <mmd:short_name>MODIS</mmd:short_name>\n      <mmd:long_name>MODIS</mmd:long_name>\n      <mmd:resource>https://www.wmo-sat.info/oscar/instruments/view/modis</mmd:resource>\n    </mmd:instrument>\n  </mmd:platform>\n  <mmd:activity_type>Space Borne Instrument</mmd:activity_type>\n  <mmd:dataset_citation>\n    <mmd:author>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:author>\n    <mmd:publication_date>2021-04-29</mmd:publication_date>\n    <mmd:title>Direct Broadcast data processed in satellite swath to L1C</mmd:title>\n  </mmd:dataset_citation>\n  <mmd:related_information>\n    <mmd:type>Dataset landing page</mmd:type>\n    <mmd:resource>https://data.met.no/dataset/a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b</mmd:resource>\n  </mmd:related_information>\n</mmd:mmd>\n'
    data_w_landingpage_andotherrelinfo=b'<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd" xmlns:gml="http://www.opengis.net/gml">\n  <mmd:metadata_identifier>test.no:a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b</mmd:metadata_identifier>\n  <mmd:title xml:lang="en">Direct Broadcast data processed in satellite swath to L1C</mmd:title>\n  <mmd:title xml:lang="nor">Direktesendte satellittdata prosessert i satellittsveip til L1C.</mmd:title>\n  <mmd:abstract xml:lang="en">Direct Broadcast data received at MET NORWAY Oslo. Processed by standard processing software to geolocated and calibrated values in satellite swath in received instrument resolution.</mmd:abstract>\n  <mmd:abstract xml:lang="nor">Direktesendte satellittdata mottatt ved Meteorologisk Institutt Oslo. Prosessert med standard prosesseringssoftware til geolokaliserte og kalibrerte verdier i satellitsveip i mottatt instrument oppl\xc3\xb8sning.</mmd:abstract>\n  <mmd:metadata_status>Active</mmd:metadata_status>\n  <mmd:dataset_production_status>Complete</mmd:dataset_production_status>\n  <mmd:collection>METNCS</mmd:collection>\n  <mmd:last_metadata_update>\n    <mmd:update>\n      <mmd:datetime>2021-04-29T00:46:05Z</mmd:datetime>\n      <mmd:type>Created</mmd:type>\n    </mmd:update>\n  </mmd:last_metadata_update>\n  <mmd:temporal_extent>\n    <mmd:start_date>2021-04-29T00:28:44.977627Z</mmd:start_date>\n    <mmd:end_date>2021-04-29T00:39:55.000000Z</mmd:end_date>\n  </mmd:temporal_extent>\n  <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>\n  <mmd:iso_topic_category>environment</mmd:iso_topic_category>\n  <mmd:iso_topic_category>oceans</mmd:iso_topic_category>\n  <mmd:keywords vocabulary="GCMD">\n    <mmd:keyword>Earth Science &gt; Atmosphere &gt; Atmospheric radiation</mmd:keyword>\n    <mmd:resource>https://gcmdservices.gsfc.nasa.gov/static/kms/</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:keywords vocabulary="GEMET">\n    <mmd:keyword>Meteorological geographical features</mmd:keyword>\n    <mmd:keyword>Atmospheric conditions</mmd:keyword>\n    <mmd:keyword>Oceanographic geographical features</mmd:keyword>\n    <mmd:resource>http://inspire.ec.europa.eu/theme</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:keywords vocabulary="Norwegian thematic categories">\n    <mmd:keyword>Weather and climate</mmd:keyword>\n    <mmd:resource>https://register.geonorge.no/subregister/metadata-kodelister/kartverket/nasjonal-temainndeling</mmd:resource>\n    <mmd:separator></mmd:separator>\n  </mmd:keywords>\n  <mmd:geographic_extent>\n    <mmd:rectangle srsName="EPSG:4326">\n      <mmd:north>80.49233</mmd:north>\n      <mmd:south>36.540688</mmd:south>\n      <mmd:east>79.40124</mmd:east>\n      <mmd:west>1.5549301</mmd:west>\n    </mmd:rectangle>\n  </mmd:geographic_extent>\n  <mmd:dataset_language>en</mmd:dataset_language>\n  <mmd:operational_status>Operational</mmd:operational_status>\n  <mmd:use_constraint>\n    <mmd:identifier>CC-BY-4.0</mmd:identifier>\n    <mmd:resource>http://spdx.org/licenses/CC-BY-4.0</mmd:resource>\n  </mmd:use_constraint>\n  <mmd:personnel>\n    <mmd:role>Technical contact</mmd:role>\n    <mmd:name>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:name>\n    <mmd:email>post@met.no</mmd:email>\n    <mmd:organisation>MET NORWAY</mmd:organisation>\n  </mmd:personnel>\n  <mmd:personnel>\n    <mmd:role>Metadata author</mmd:role>\n    <mmd:name>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:name>\n    <mmd:email>post@met.no</mmd:email>\n    <mmd:organisation>unknown</mmd:organisation>\n  </mmd:personnel>\n  <mmd:data_center>\n    <mmd:data_center_name>\n      <mmd:short_name>MET NORWAY</mmd:short_name>\n      <mmd:long_name>MET NORWAY</mmd:long_name>\n    </mmd:data_center_name>\n    <mmd:data_center_url>met.no</mmd:data_center_url>\n  </mmd:data_center>\n  <mmd:data_access>\n    <mmd:type>OPeNDAP</mmd:type>\n    <mmd:description>Open-source Project for a Network Data Access Protocol</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/dodsC/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:resource>\n  </mmd:data_access>\n  <mmd:data_access>\n    <mmd:type>OGC WMS</mmd:type>\n    <mmd:description>OGC Web Mapping Service, URI to GetCapabilities Document.</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/wms/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc?service=WMS&amp;version=1.3.0&amp;request=GetCapabilities</mmd:resource>\n    <mmd:wms_layers>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_brightness_temperature</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n        <mmd:wms_layer>toa_bidirectional_reflectance</mmd:wms_layer>\n    </mmd:wms_layers>\n  </mmd:data_access>\n  <mmd:data_access>\n    <mmd:type>HTTP</mmd:type>\n    <mmd:description>Direct download of file</mmd:description>\n    <mmd:resource>https://thredds.met.no/thredds/fileServer/remotesensingsatellite/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:resource>\n  </mmd:data_access>\n  <mmd:related_dataset relation_type="parent">test.no:64db6102-14ce-41e9-b93b-61dbb2cb8b4e</mmd:related_dataset>\n  <mmd:storage_information>\n    <mmd:file_name>aqua-modis-1km-20210429002844-20210429003955.nc</mmd:file_name>\n    <mmd:file_location>/lustre/storeB/immutable/archive/projects/remotesensing/satellite-thredds/polar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc</mmd:file_location>\n    <mmd:file_format>NetCDF-CF</mmd:file_format>\n    <mmd:file_size unit="MB">1862.00</mmd:file_size>\n    <mmd:checksum type="md5sum">4e1833610272ee63228f575d1c875fbe</mmd:checksum>\n  </mmd:storage_information>\n  <mmd:project>\n    <mmd:short_name>Govermental core service</mmd:short_name>\n    <mmd:long_name>Govermental core service</mmd:long_name>\n  </mmd:project>\n  <mmd:platform>\n    <mmd:short_name>Aqua</mmd:short_name>\n    <mmd:long_name>Aqua</mmd:long_name>\n    <mmd:resource>https://www.wmo-sat.info/oscar/satellites/view/aqua</mmd:resource>\n    <mmd:instrument>\n      <mmd:short_name>MODIS</mmd:short_name>\n      <mmd:long_name>MODIS</mmd:long_name>\n      <mmd:resource>https://www.wmo-sat.info/oscar/instruments/view/modis</mmd:resource>\n    </mmd:instrument>\n  </mmd:platform>\n  <mmd:activity_type>Space Borne Instrument</mmd:activity_type>\n  <mmd:dataset_citation>\n    <mmd:author>DIVISION FOR OBSERVATION QUALITY AND DATA PROCESSING</mmd:author>\n    <mmd:publication_date>2021-04-29</mmd:publication_date>\n    <mmd:title>Direct Broadcast data processed in satellite swath to L1C</mmd:title>\n  </mmd:dataset_citation>\n  <mmd:related_information>\n    <mmd:type>Dataset landing page</mmd:type>\n    <mmd:resource>https://data.met.no/dataset/a1ddaf0f-cae0-4a15-9b37-3468e9cb1a2b</mmd:resource>\n    <mmd:type>Project home page</mmd:type>\n    <mmd:resource>https://thisfieldislegit.com</mmd:resource>\n  </mmd:related_information>\n</mmd:mmd>\n'
   
    tstWorker = Worker("insert", passFile, None)

    assert tstWorker._add_landing_page(data_wo_landingpage,catalog_url) == data_w_landingpage
    assert tstWorker._add_landing_page(data_w_relinf_nolandingpage,catalog_url) == data_w_landingpage_andotherrelinfo
    assert tstWorker._add_landing_page(data_w_old_landingpage,catalog_url) == data_w_landingpage

# END Test testApiWorker_AddLandingPage



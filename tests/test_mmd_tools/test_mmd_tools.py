# -*- coding: utf-8 -*-
"""
DMCI : MMD Tools Module Tests
=============================

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

from lxml import etree

from dmci.mmd_tools import CheckMMD

@pytest.mark.mmd_tools
def testMMDTools_CheckRectangle():
    """Test the check_rectangle function.
    """
    chkMMD = CheckMMD()
    etreeRef = etree.ElementTree(etree.XML(
        "<root>"
        "  <resource>https://www.met.no/</resource>"
        "  <geographic_extent>"
        "    <rectangle>"
        "      <north>76.199661</north>"
        "      <south>71.63427</south>"
        "      <west>-28.114723</west>"
        "      <east>-11.169785</east>"
        "    </rectangle>"
        "  </geographic_extent>"
        "</root>"
    ))

    # Check lat/lon OK from rectangle
    rect = etreeRef.findall("./{*}geographic_extent/{*}rectangle")
    assert chkMMD.check_rectangle(rect) == (True, [])

    # Check longitude NOK
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "20"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "west").text = "50"
    etree.SubElement(root, "east").text = "0"
    ok, err = chkMMD.check_rectangle([root])
    assert ok is False
    assert err == ["Longitudes not in range -180 <= west <= east <= 180."]

    # Check latitude NOK
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "-182"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "west").text = "0"
    etree.SubElement(root, "east").text = "180"
    ok, err = chkMMD.check_rectangle([root])
    assert ok is False
    assert err == ["Latitudes not in range -90 <= south <= north <= 90."]

    # Check more than one rectangle as input
    ok, err = chkMMD.check_rectangle(["elem1", "elem2"])
    assert ok is False
    assert err == ["Multiple rectangle elements in file."]

    # Check lat & long OK with namespace
    root = etree.Element("rectangle")
    etree.SubElement(root, "{http://www.met.no/schema/mmd}south").text = "20"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}north").text = "50"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}west").text = "0"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}east").text = "50"
    assert chkMMD.check_rectangle([root]) == (True, [])

    # Check rectangle with one missing element (no west)
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "-182"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "east").text = "180"
    ok, err = chkMMD.check_rectangle([root])
    assert ok is False
    assert err == ["Missing rectangle element 'west'."]

    # Check rectangle with element with typo
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "20"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "west").text = "0"
    etree.SubElement(root, "easttt").text = "50"
    ok, err = chkMMD.check_rectangle([root])
    assert ok is False
    assert err == ["Missing rectangle element 'east'."]

# END Test testMMDTools_CheckRectangle

@pytest.mark.mmd_tools
def testMMDTools_CheckURLs():
    """Test the check_url function.
    """
    chkMMD = CheckMMD()

    # Valid URL
    ok, err = chkMMD.check_url("https://www.met.no/")
    assert ok is True
    assert err == []

    # Schemes
    ok, err = chkMMD.check_url("https://www.met.no/")
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("http://www.met.no/")
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("file://www.met.no/")
    assert ok is False
    assert err == ["URL scheme 'file' not allowed."]

    ok, err = chkMMD.check_url("imap://www.met.no/")
    assert ok is False
    assert err == ["URL scheme 'imap' not allowed."]

    ok, err = chkMMD.check_url("stuff://www.met.no/")
    assert ok is False
    assert err == ["URL scheme 'stuff' not allowed."]

    # Domains
    ok, err = chkMMD.check_url("https://www.met.no/")
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("https://met.no/")
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("https:/www.met.no/")
    assert ok is False
    assert err == ["Domain '' is not valid."]

    ok, err = chkMMD.check_url("https://metno/")
    assert ok is False
    assert err == ["Domain 'metno' is not valid."]

    # Path
    ok, err = chkMMD.check_url("https://www.met.no", allow_no_path=True)
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("https://www.met.no")
    assert ok is False
    assert err == ["URL contains no path. At least '/' is required."]

    ok, err = chkMMD.check_url("https://www.met.no/")
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_url("https://www.met.no/location")
    assert ok is True
    assert err == []

    # Non-ASCII characters
    ok, err = chkMMD.check_url("https://www.mæt.no/")
    assert ok is False
    assert err == ["URL contains non-ASCII characters."]

    # Unparsable
    ok, err = chkMMD.check_url(12345)
    assert ok is False
    assert err == ["URL contains non-ASCII characters.", "URL cannot be parsed by urllib."]

# END Test testMMDTools_CheckURLs

@pytest.mark.mmd_tools
def testMMDTools_CheckCF():
    """Test the check_cf function.
    """
    chkMMD = CheckMMD()

    ok, err, n = chkMMD.check_cf(etree.ElementTree(etree.XML(
        "<root>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surface_temperature</keyword>"
        "  </keywords>"
        "</root>"
    )))
    assert ok is True
    assert err == []
    assert n == 1

    ok, err, n = chkMMD.check_cf(etree.ElementTree(etree.XML(
        "<root>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surface_temperature</keyword>"
        "    <keyword>sea_surface_temperature</keyword>"
        "  </keywords>"
        "</root>"
    )))
    assert ok is False
    assert err == ["Only one CF name should be provided, got 2."]
    assert n == 1

    ok, err, n = chkMMD.check_cf(etree.ElementTree(etree.XML(
        "<root>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surace_temperature</keyword>"
        "  </keywords>"
        "</root>"
    )))
    assert ok is False
    assert err == ["Keyword 'sea_surace_temperature' is not a CF standard name."]
    assert n == 1

    ok, err, n = chkMMD.check_cf(etree.ElementTree(etree.XML(
        "<root>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surface_temperature</keyword>"
        "  </keywords>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surface_temperature</keyword>"
        "  </keywords>"
        "</root>"
    )))
    assert ok is False
    assert err == ["More than one CF entry found. Only one is allowed."]
    assert n == 2

# END Test testMMDTools_CheckCF

@pytest.mark.mmd_tools
def testMMDTools_CheckVocabulary():
    """Test the check_vocabulary function.
    """
    chkMMD = CheckMMD()
    ok, err = chkMMD.check_vocabulary(etree.ElementTree(etree.XML(
        "<root><operational_status>Operational</operational_status></root>"
    )))
    assert ok is True
    assert err == []

    ok, err = chkMMD.check_vocabulary(etree.ElementTree(etree.XML(
        "<root><operational_status>OOperational</operational_status></root>"
    )))
    assert ok is False
    assert err == ["Incorrect vocabulary 'OOperational' for element 'operational_status'."]

# END Test testMMDTools_CheckVocabulary

@pytest.mark.mmd_tools
def testMMDTools_FullCheck(filesDir):
    """Test the full_check function.
    """
    chkMMD = CheckMMD()
    passFile = os.path.join(filesDir, "api", "passing.xml")
    passTree = etree.parse(passFile, parser=etree.XMLParser(remove_blank_text=True))

    # Full check
    assert chkMMD.full_check(passTree) is True
    ok, msgs = chkMMD.status()
    assert ok is True
    assert "\n".join(msgs) == (
        "Passed: URL Check on 'https://gcmdservices.gsfc.nasa.gov/static/kms/'\n"
        "Passed: URL Check on 'http://inspire.ec.europa.eu/theme'\n"
        "Passed: URL Check on 'https://register.geonorge.no/subregister/metadata-kodelister/kartve"
        "rket/nasjonal-temainndeling'\n"
        "Passed: URL Check on 'http://spdx.org/licenses/CC-BY-4.0'\n"
        "Passed: URL Check on 'https://thredds.met.no/thredds/dodsC/remotesensingsatellite/polar-s"
        "wath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc'\n"
        "Passed: URL Check on 'https://thredds.met.no/thredds/wms/remotesensingsatellite/polar-swa"
        "th/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc?service=WMS&version=1.3.0&r"
        "equest=GetCapabilities'\n"
        "Passed: URL Check on 'https://thredds.met.no/thredds/fileServer/remotesensingsatellite/po"
        "lar-swath/2021/04/29/aqua-modis-1km-20210429002844-20210429003955.nc'\n"
        "Passed: URL Check on 'https://www.wmo-sat.info/oscar/satellites/view/aqua'\n"
        "Passed: URL Check on 'https://www.wmo-sat.info/oscar/instruments/view/modis'\n"
        "Passed: Rectangle Check\n"
        "Passed: Controlled Vocabularies Check\n"
    ).rstrip()

    # Full check with no elements to check
    assert chkMMD.full_check(etree.ElementTree(etree.XML("<xml />"))) is True
    ok, msgs = chkMMD.status()
    assert ok is True
    assert "\n".join(msgs) == ""

    # Full check with invalid elements
    etreeUrlRectNok = etree.ElementTree(etree.XML(
        "<root>"
        "  <resource>https://www.mæt.no/</resource>"
        "  <geographic_extent>"
        "    <rectangle>"
        "      <north>76.199661</north>"
        "      <south>71.63427</south>"
        "      <west>-28.114723</west>"
        "    </rectangle>"
        "  </geographic_extent>"
        "  <keywords vocabulary='Climate and Forecast Standard Names'>"
        "    <keyword>sea_surface_temperature</keyword>"
        "    <keyword>air_surface_temperature</keyword>"
        "  </keywords>"
        "  <operational_status>NotOpen</operational_status>"
        "</root>"
    ))
    assert chkMMD.full_check(etreeUrlRectNok) is False
    ok, msgs = chkMMD.status()
    assert ok is False
    assert "\n".join(msgs) == (
        "Failed: URL Check on 'https://www.mæt.no/'\n"
        " - URL contains non-ASCII characters.\n"
        "Failed: Climate and Forecast Standard Names Check\n"
        " - Only one CF name should be provided, got 2.\n"
        " - Keyword 'air_surface_temperature' is not a CF standard name.\n"
        "Failed: Controlled Vocabularies Check\n"
        " - Incorrect vocabulary 'NotOpen' for element 'operational_status'.\n"
    ).rstrip()

# END Test testMMDTools_FullCheck

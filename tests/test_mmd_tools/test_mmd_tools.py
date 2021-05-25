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

import pytest
import logging

from lxml import etree

from dmci.mmd_tools.check_mmd import (
    check_rectangle, check_urls, check_cf, check_vocabulary, full_check
)

etreeRef = etree.ElementTree(etree.XML(
    "<root>"
    "  <a x='123'>https://www.met.no</a>"
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

etreeRefEmpty = etree.ElementTree(etree.XML(
    "<root>"
    "  <a x=\"123\">'xxx'/><c/><b/></a>"
    "</root>"
))

etreeUrlRectNok = etree.ElementTree(etree.XML(
    "<root>"
    "  <a x='123'>https://www.mæt.no</a>"
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

@pytest.mark.mmd_tools
def testMMDTools_CheckRectangle(caplog):
    """Test the check_rectangle function.
    """
    caplog.set_level(logging.DEBUG, logger="dmci")

    # Check lat/lon OK from rectangle
    rect = etreeRef.findall("./{*}geographic_extent/{*}rectangle")
    assert check_rectangle(rect) is True

    # Check longitude NOK
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "20"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "west").text = "50"
    etree.SubElement(root, "east").text = "0"
    assert check_rectangle([root]) is False
    assert "Longitudes not ok" in caplog.text

    # Check latitude NOK
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "-182"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "west").text = "0"
    etree.SubElement(root, "east").text = "180"
    assert check_rectangle([root]) is False
    assert "Latitudes not ok" in caplog.text

    # Check more than one rectangle as input
    assert check_rectangle(["elem1", "elem2"]) is False
    assert "Multiple rectangle elements in file" in caplog.text

    # Check lat & long OK with namespace
    root = etree.Element("rectangle")
    etree.SubElement(root, "{http://www.met.no/schema/mmd}south").text = "20"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}north").text = "50"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}west").text = "0"
    etree.SubElement(root, "{http://www.met.no/schema/mmd}east").text = "50"
    assert check_rectangle([root]) is True

    # Check rectangle with one missing element (no west)
    root = etree.Element("rectangle")
    etree.SubElement(root, "south").text = "-182"
    etree.SubElement(root, "north").text = "50"
    etree.SubElement(root, "east").text = "180"
    assert check_rectangle([root]) is False
    assert "Missing rectangle element west" in caplog.text

# END Test testMMDTools_CheckRectangle

@pytest.mark.mmd_tools
def testMMDTools_CheckURLs(monkeypatch):
    """Test the check_urls function.
    """
    # Valid URL
    assert check_urls(["https://www.met.no/"]) is True

    # Schemes
    assert check_urls(["https://www.met.no/"]) is True
    assert check_urls(["http://www.met.no/"]) is True
    assert check_urls(["file://www.met.no/"]) is False
    assert check_urls(["imap://www.met.no/"]) is False
    assert check_urls(["stuff://www.met.no/"]) is False

    # Domains
    assert check_urls(["https://www.met.no/"]) is True
    assert check_urls(["https://met.no/"]) is True
    assert check_urls(["https:/www.met.no/"]) is False
    assert check_urls(["https://metno/"]) is False

    # Path
    assert check_urls(["https://www.met.no"], allow_no_path=True) is True
    assert check_urls(["https://www.met.no"]) is False
    assert check_urls(["https://www.met.no/"]) is True
    assert check_urls(["https://www.met.no/location"]) is True

    # Non-ASCII characters
    assert check_urls(["https://www.mæt.no/"]) is False

    # Unparsable
    assert check_urls([12345]) is False

# END Test testMMDTools_CheckURLs

@pytest.mark.mmd_tools
def off_testMMDTools_CheckCF():
    """Test the check_cf function.
    """
    assert check_cf(["sea_surface_temperature"]) is True
    assert check_cf(["sea_surace_temperature"]) is False

# END Test testMMDTools_CheckCF

@pytest.mark.mmd_tools
def off_testMMDTools_CheckVocabulary():
    """Test the check_vocabulary function.
    """
    assert check_vocabulary(etree.ElementTree(etree.XML(
        "<root><operational_status>Operational</operational_status></root>"
    ))) is True

    assert check_vocabulary(etree.ElementTree(etree.XML(
        "<root><operational_status>OOperational</operational_status></root>"
    ))) is False

# END Test testMMDTools_CheckVocabulary

@pytest.mark.mmd_tools
def testMMDTools_FullCheck():
    """Test the full_check function.
    """
    # Full check
    assert full_check(etreeRef) is True

    # Full check with no elements to check
    assert full_check(etreeRefEmpty) is True

    # Full check with invalid elements
    assert full_check(etreeUrlRectNok) is False

    # Twice the element keywords for the same vocabulary
    # root = etree.Element("toto")
    # key1 = etree.SubElement(root, "keywords", vocabulary="Climate and Forecast Standard Names")
    # etree.SubElement(key1, "keyword").text = "air_temperature"
    # key2 = etree.SubElement(root, "keywords", vocabulary="Climate and Forecast Standard Names")
    # etree.SubElement(key2, "keyword").text = "air_temperature"
    # assert full_check(root) is False

    # Correct case
    # root = etree.Element("toto")
    # root1 = etree.SubElement(root, "keywords", vocabulary="Climate and Forecast Standard Names")
    # etree.SubElement(root1, "keyword").text = "sea_surface_temperature"
    # assert full_check(root) is True

    # Two standard names provided
    # root = etree.Element("toto")
    # root1 = etree.SubElement(root, "keywords", vocabulary="Climate and Forecast Standard Names")
    # etree.SubElement(root1, "keyword").text = "air_temperature"
    # etree.SubElement(root1, "keyword").text = "sea_surface_temperature"
    # assert full_check(root) is False

# END Test testMMDTools_FullCheck

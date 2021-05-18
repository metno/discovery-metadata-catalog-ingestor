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

from dmci.mmd_tools.check_mmd import check_rectangle

@pytest.fixture(scope="function")
def etreeRef():
    return etree.ElementTree(etree.XML(
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

@pytest.mark.mmd_tools
def testMMDTools_CheckRectangle(etreeRef, caplog):
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

# -*- coding: utf-8 -*-
"""
DMCI : Distributor Class Test
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

from tools import writeFile

from dmci.distributors.distributor import Distributor

@pytest.mark.dist
def testDistDistributor_Init(tmpDir):
    """Test the Distributor super class init.
    """
    distDir = os.path.join(tmpDir, "dist")
    os.mkdir(distDir)

    # Check commands
    assert Distributor("insert", metadata_id="some_id").is_valid() is True
    assert Distributor("update", metadata_id="some_id").is_valid() is True
    assert Distributor("delete", metadata_id="some_id").is_valid() is True
    assert Distributor("iNseRt", metadata_id="some_id").is_valid() is True
    assert Distributor("upDaTe", metadata_id="some_id").is_valid() is True
    assert Distributor("DelEte", metadata_id="some_id").is_valid() is True
    assert Distributor("blabla", metadata_id="some_id").is_valid() is False
    assert Distributor(1234.567, metadata_id="some_id").is_valid() is False

    # Test XML File Flag and Metadata ID
    dummyXml = os.path.join(distDir, "dummy.xml")
    writeFile(dummyXml, "<xml />")
    assert Distributor("insert", xml_file=dummyXml).is_valid() is True
    assert Distributor("insert", xml_file="/path/to/nowhere").is_valid() is False

    assert Distributor("insert", metadata_id="stuff").is_valid() is True
    assert Distributor("insert", metadata_id=1234.56).is_valid() is False
    assert Distributor("insert", metadata_id="").is_valid() is False

    # Cannot set neither or both file and ID
    assert Distributor("insert").is_valid() is False
    assert Distributor("insert", xml_file=dummyXml, metadata_id="stuff").is_valid() is False

# END Test testDistDistributor_Init

@pytest.mark.dist
def testDistDistributor_Run(tmpDir):
    """Test the Distributor super class run function.
    Calling run() on the superclass should always return False as
    nothing is actually run.
    """
    assert Distributor("insert", metadata_id="some_id").run() is True
    assert Distributor("blabla", metadata_id="some_id").run() is False

# END Test testDistDistributor_Run

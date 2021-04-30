# -*- coding: utf-8 -*-
"""
DMCI : Test Config
==================

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
import sys
import shutil
import pytest

from tools import writeFile

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

@pytest.fixture(scope="session")
def tmpDir():
    """A temporary folder for the test session. This folder is
    presistent after the tests have run so that the status of generated
    files can be checked. The folder is instead cleared before a new
    test session.
    """
    testDir = os.path.dirname(__file__)
    theDir = os.path.join(testDir, "temp")
    if os.path.isdir(theDir):
        shutil.rmtree(theDir)
    if not os.path.isdir(theDir):
        os.mkdir(theDir)
    return theDir

@pytest.fixture(scope="session")
def refDir():
    """A path to the reference files folder.
    """
    testDir = os.path.dirname(__file__)
    theDir = os.path.join(testDir, "files")
    return theDir

@pytest.fixture(scope='session')
def dummy_xml_str():
    return "<xml />"

@pytest.fixture(scope='session')
def distDir(tmpDir):
    distDir = os.path.join(tmpDir, "dist")
    if not os.path.isdir(distDir):
        os.mkdir(distDir)
    return distDir

@pytest.fixture(scope='session')
def dummyXml(distDir, dummy_xml_str):
    dummyXml = os.path.join(distDir, "dummy.xml")
    writeFile(dummyXml, dummy_xml_str)
    return dummyXml

@pytest.fixture(scope='session')
def dummyXslt(distDir):
    dummyXslt = os.path.join(distDir, "dummy.xslt")
    writeFile(dummyXslt, "<xml />")
    return dummyXslt


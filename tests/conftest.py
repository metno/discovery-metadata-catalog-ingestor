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

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from dmci.config import Config  # noqa: E402


##
#  Directory Fixtures
##

@pytest.fixture(scope="session")
def rootDir():
    """The root folder of the repository."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))


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
def filesDir():
    """A path to the reference files folder."""
    testDir = os.path.dirname(__file__)
    theDir = os.path.join(testDir, "files")
    return theDir


@pytest.fixture(scope="function")
def fncDir(tmpDir):
    """A temporary folder for a single test function."""
    fncDir = os.path.join(tmpDir, "f_temp")
    if os.path.isdir(fncDir):
        shutil.rmtree(fncDir)
    if not os.path.isdir(fncDir):
        os.mkdir(fncDir)
    return fncDir


##
#  Mock Files
##

@pytest.fixture(scope="session")
def mockXml(filesDir):
    return os.path.join(filesDir, "mock", "mock.xml")


@pytest.fixture(scope="session")
def mockXslt(filesDir):
    return os.path.join(filesDir, "mock", "mock.xslt")


@pytest.fixture(scope="session")
def mockXsd(filesDir):
    return os.path.join(filesDir, "mock", "mock.xsd")


##
#  Objects
##

@pytest.fixture(scope="function")
def tmpConf(monkeypatch):
    """Create a temporary configuration object."""
    theConf = Config()
    confFile = os.path.join(theConf.pkg_root, "example_config.yaml")
    theConf.readConfig(confFile)
    monkeypatch.setattr("dmci.CONFIG", theConf)
    return theConf

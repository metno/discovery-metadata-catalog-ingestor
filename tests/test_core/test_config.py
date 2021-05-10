# -*- coding: utf-8 -*-
"""
DMCI : Config Class Test
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
import pytest

from tools import causeOSError

from dmci.config import Config

@pytest.mark.core
def testCoreConfig_ReadFile(filesDir, monkeypatch):
    """Test reading config file.
    """
    theConf = Config()

    # Read some values and see that we get them
    confFile = os.path.join(filesDir, "core", "config.yaml")

    # Fake path
    assert not theConf.readConfig(configFile="not_a_real_file")

    # Cause the open command to fail
    with monkeypatch.context() as mp:
        mp.setattr("builtins.open", causeOSError)
        assert not theConf.readConfig(configFile=confFile)

    # Successful raw read
    theConf.readConfig(configFile=confFile)

    # Check the values read
    assert theConf._raw_conf["groupOne"]["keyOne"] == 1
    assert theConf._raw_conf["groupOne"]["keyTwo"] == "two"
    assert theConf._raw_conf["groupOne"]["keyThree"] is None
    assert theConf._raw_conf["groupOne"]["keyFour"] == ["value1", "value2"]

    # Read with no file path set, but a folder that contains the test file
    theConf.pkg_root = os.path.join(filesDir, "core")
    theConf.readConfig(configFile=None)
    assert theConf._raw_conf["groupOne"]["keyOne"] == 1
    assert theConf._raw_conf["groupOne"]["keyTwo"] == "two"
    assert theConf._raw_conf["groupOne"]["keyThree"] is None
    assert theConf._raw_conf["groupOne"]["keyFour"] == ["value1", "value2"]

# END Test testCoreConfig_ReadFile

@pytest.mark.core
def testCoreConfig_Validate(rootDir, filesDir, tmpDir):
    """Test that the class reads all settings and validates them.
    """
    exampleConf = os.path.join(rootDir, "example_config.yaml")
    theConf = Config()

    # Set wrong values
    theConf.call_distributors = []
    theConf.distributor_cache = "path"
    theConf.max_permitted_size = 0
    theConf.mmd_xslt_path = "path"
    theConf.mmd_xsd_path = "path"
    theConf.git_jobs_path = "path"

    # Check the values from example_config.yaml are read
    theConf.readConfig(configFile=exampleConf)

    assert theConf.call_distributors == ["git", "pycsw"]
    assert theConf.distributor_cache is None
    assert theConf.max_permitted_size == 100000
    assert theConf.mmd_xslt_path is None
    assert theConf.mmd_xsd_path is None
    assert theConf.git_jobs_path is None

    assert theConf.csw_service_url == "localhost"

    # Set valid values
    theConf.mmd_xsd_path = os.path.join(filesDir, "mmd", "mmd.xsd")
    theConf.mmd_xslt_path = os.path.join(filesDir, "mmd", "mmd-to-geonorge.xslt")
    theConf.git_jobs_path = tmpDir
    assert theConf._validate_config() is True

    # Validate XSD Path
    correctVal = theConf.mmd_xsd_path
    theConf.mmd_xsd_path = None
    assert theConf._validate_config() is False
    theConf.mmd_xsd_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.mmd_xsd_path = correctVal
    assert theConf._validate_config() is True

    # Validate XSLT Path
    correctVal = theConf.mmd_xslt_path
    theConf.mmd_xslt_path = None
    assert theConf._validate_config() is False
    theConf.mmd_xslt_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.mmd_xslt_path = correctVal
    assert theConf._validate_config() is True

    # Validate Git Jobs Path
    correctVal = theConf.git_jobs_path
    theConf.git_jobs_path = None
    assert theConf._validate_config() is False
    theConf.git_jobs_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.git_jobs_path = correctVal
    assert theConf._validate_config() is True

# END Test testCoreConfig_Validate

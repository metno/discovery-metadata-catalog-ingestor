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
    """Test reading config file."""
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
    """Test that the class reads all settings and validates them."""
    exampleConf = os.path.join(rootDir, "example_config.yaml")
    theConf = Config()

    # Set wrong values
    theConf.call_distributors = []
    theConf.distributor_cache = "path"
    theConf.rejected_jobs_path = "path"
    theConf.max_permitted_size = 0
    theConf.mmd_xsl_path = "path"
    theConf.mmd_xsd_path = "path"
    theConf.file_archive_path = "path"
    theConf.path_to_parent_list = "path"

    # Check the values from example_config.yaml are read
    theConf.readConfig(configFile=exampleConf)

    assert theConf.call_distributors == ["file", "pycsw"]
    assert theConf.distributor_cache is None
    assert theConf.rejected_jobs_path is None
    assert theConf.max_permitted_size == 100000
    assert theConf.mmd_xsl_path is None
    assert theConf.mmd_xsd_path is None
    assert theConf.file_archive_path is None
    assert theConf.path_to_parent_list is None

    assert theConf.csw_service_url == "http://localhost"

    # Set valid values
    theConf.mmd_xsd_path = os.path.join(filesDir, "mmd", "mmd.xsd")
    theConf.mmd_xsl_path = os.path.join(filesDir, "mmd", "mmd-to-geonorge.xsl")
    theConf.distributor_cache = tmpDir
    theConf.rejected_jobs_path = tmpDir
    theConf.file_archive_path = tmpDir
    theConf.path_to_parent_list = os.path.join(filesDir, "mmd", "parent-uuid-list.xml")
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
    correctVal = theConf.mmd_xsl_path
    theConf.mmd_xsl_path = None
    assert theConf._validate_config() is False
    theConf.mmd_xsl_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.mmd_xsl_path = correctVal
    assert theConf._validate_config() is True

    # Validate File Archive Path
    correctVal = theConf.file_archive_path
    theConf.file_archive_path = None
    assert theConf._validate_config() is False
    theConf.file_archive_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.file_archive_path = correctVal
    assert theConf._validate_config() is True

    # Validate Distributor Cache
    correctVal = theConf.distributor_cache
    theConf.distributor_cache = None
    assert theConf._validate_config() is False
    theConf.distributor_cache = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.distributor_cache = correctVal
    assert theConf._validate_config() is True

    # Validate Rejected Folder
    correctVal = theConf.rejected_jobs_path
    theConf.rejected_jobs_path = None
    assert theConf._validate_config() is False
    theConf.rejected_jobs_path = "path/to/nowhere"
    assert theConf._validate_config() is False
    theConf.rejected_jobs_path = correctVal
    assert theConf._validate_config() is True

# END Test testCoreConfig_Validate

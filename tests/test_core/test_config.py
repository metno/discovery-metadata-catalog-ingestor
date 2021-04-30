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

    # Successful read
    assert theConf.readConfig(configFile=confFile)

    # Check the values read
    assert theConf._raw_conf["groupOne"]["keyOne"] == 1
    assert theConf._raw_conf["groupOne"]["keyTwo"] == "two"
    assert theConf._raw_conf["groupOne"]["keyThree"] is None
    assert theConf._raw_conf["groupOne"]["keyFour"] == ["value1", "value2"]

    # Read with no file path set, but a folder that contains the test file
    theConf.pkg_root = os.path.join(filesDir, "core")
    assert theConf.readConfig(configFile=None)

# END Test testCoreConfig_ReadFile

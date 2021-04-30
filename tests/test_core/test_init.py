# -*- coding: utf-8 -*-
"""
DMCI : Package Init Test
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
import logging

import dmci

@pytest.mark.core
def testCoreInit_Init():
    """Test the package initialisation.
    """
    assert dmci.__version__
    assert isinstance(dmci.CONFIG, dmci.config.Config)

# END Test testCoreInit_Init

@pytest.mark.core
def testCoreInit_Logger():
    """Test the logger initialisation.
    """
    os.environ["DMCI_LOGLEVEL"] = "DEBUG"
    logger = logging.getLogger(__name__)
    dmci._init_logging(logger)
    assert logger.getEffectiveLevel() == logging.DEBUG

    os.environ["DMCI_LOGLEVEL"] = "INVALID"
    logger = logging.getLogger(__name__)
    dmci._init_logging(logger)
    assert logger.getEffectiveLevel() == logging.INFO

# END Test testCoreInit_Logger

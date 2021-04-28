# -*- coding: utf-8 -*-
"""
DMCI : Main Package Init
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
import logging

from dmci.config import Config

__package__ = "dmci"
__version__ = "0.0.1"

def _initLogging(logObj):
    """Call to initialise logging
    """
    strLevel = os.environ.get("DMCI_LOGLEVEL", "INFO")
    if hasattr(logging, strLevel):
        logLevel = getattr(logging, strLevel)
    else:
        print("Invalid logging level '%s' in environment variable DMCI_LOGLEVEL" % strLevel)
        logLevel = logging.INFO

    if logLevel < logging.INFO:
        logFormat = "[{asctime:s}] {levelname:8s} {message:}"
    else:
        logFormat = "{levelname:8s} {message:}"

    logFmt = logging.Formatter(fmt=logFormat, style="{")

    cHandle = logging.StreamHandler()
    cHandle.setLevel(logLevel)
    cHandle.setFormatter(logFmt)

    logObj.setLevel(logLevel)
    logObj.addHandler(cHandle)

    return

# Logging Setup
logger = logging.getLogger(__name__)
_initLogging(logger)

# Create config object
CONFIG = Config()

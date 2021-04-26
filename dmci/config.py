# -*- coding: utf-8 -*-
"""
DMCI : Main Config
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
import yaml
import logging

logger = logging.getLogger(__name__)

class Config():

    def __init__(self):

        # Paths
        self.pkgRoot = os.path.abspath(os.path.dirname(__file__))

        # Internals
        self._rawConf = {}

        return

    def readConfig(self, configFile=None):
        """Read the config file. If the configFile variable is not set,
        the class will look for the file in the source root folder.
        """
        if configFile is None:
            configFile = os.path.join(self.pkgRoot, "config.yaml")

        if not os.path.isfile(configFile):
            logger.error("Config file not found: %s" % configFile)
            return False

        try:
            with open(configFile, mode="r", encoding="utf8") as inFile:
                self._rawConf = yaml.safe_load(inFile)
        except Exception as e:
            logger.error("Could not read file: %s" % configFile)
            logger.error(str(e))
            return False

        return True

# END Class Config

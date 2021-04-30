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
        self.pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

        # Core Values
        self.call_distributors = []
        self.csw_service_url = 'https://csw-dev.s-enda.k8s.met.no'

        # API
        self.distributor_input_path = None
        self.mmd_xsd_schema = os.path.join(self.pkg_root, 'dmci/assets/mmd.xsd')

        # Size of files permitted through API, 100 KB
        self.max_permitted_size = 100*1000

        # Internals
        self._raw_conf = {}

        return

    def readConfig(self, configFile=None):
        """Read the config file. If the configFile variable is not set,
        the class will look for the file in the source root folder.
        """
        if configFile is None:
            configFile = os.path.join(self.pkg_root, "config.yaml")

        if not os.path.isfile(configFile):
            logger.error("Config file not found: %s" % configFile)
            return False

        try:
            with open(configFile, mode="r", encoding="utf8") as inFile:
                self._raw_conf = yaml.safe_load(inFile)
            logger.debug("Read config from: %s" % configFile)
        except Exception as e:
            logger.error("Could not read file: %s" % configFile)
            logger.error(str(e))
            return False

        # Read Values
        valid = True
        valid &= self._read_core()

        return valid

    ##
    #  Internal Functions
    ##

    def _read_core(self):
        """Read config values under 'dmci'.
        """
        dmciDict = self._raw_conf.get("dmci", {})

        self.call_distributors = dmciDict.get("distributors", self.call_distributors)
        self.csw_service_url = dmciDict.get("csw_service_url", self.csw_service_url)
        self.distributor_input_path = dmciDict.get("distributor_input_path", None)
        self.max_permitted_size = dmciDict.get("max_permitted_size", self.max_permitted_size)
        return True

# END Class Config

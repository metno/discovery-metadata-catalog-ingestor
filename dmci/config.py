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
        self.distributor_cache = None
        self.max_permitted_size = 100000  # Size of files permitted through API
        self.mmd_xslt_path = None
        self.mmd_xsd_path = None

        # PyCSW Distributor
        self.csw_service_url = None

        # File Distributor
        self.file_archive_path = None

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
        self._read_core()
        self._read_pycsw()
        self._read_file()

        valid = self._validate_config()

        return valid

    ##
    #  Internal Functions
    ##

    def _read_core(self):
        """Read config values under 'dmci'."""
        conf = self._raw_conf.get("dmci", {})

        self.call_distributors = conf.get("distributors", self.call_distributors)
        self.distributor_cache = conf.get("distributor_cache", self.distributor_cache)
        self.max_permitted_size = conf.get("max_permitted_size", self.max_permitted_size)
        self.mmd_xslt_path = conf.get("mmd_xslt_path", self.mmd_xslt_path)
        self.mmd_xsd_path = conf.get("mmd_xsd_path", self.mmd_xsd_path)

        return

    def _read_pycsw(self):
        """Read config values under 'pycsw'."""
        conf = self._raw_conf.get("pycsw", {})

        self.csw_service_url = conf.get("csw_service_url", self.csw_service_url)

        return

    def _read_file(self):
        """Read config values under 'pycsw'."""
        conf = self._raw_conf.get("file", {})

        self.file_archive_path = conf.get("file_archive_path", self.file_archive_path)

        return

    def _validate_config(self):
        """Check config variable dependencies. It needs to be called
        after all the read functions when all settings have been
        handled.
        """
        valid = True

        if "pycsw" in self.call_distributors:
            if self.mmd_xslt_path is None:
                logger.error("Config value 'mmd_xslt_path' must be set for the pycsw distributor")
                valid = False
            else:
                if not os.path.isfile(self.mmd_xslt_path):
                    logger.error("Config value 'mmd_xslt_path' must point to an existing file")
                    valid = False

        if self.mmd_xsd_path is None:
            logger.error("Config value 'mmd_xsd_path' must be set")
            valid = False
        else:
            if not os.path.isfile(self.mmd_xsd_path):
                logger.error("Config value 'mmd_xsd_path' must point to an existing file")
                valid = False

        if "file" in self.call_distributors:
            if self.file_archive_path is None:
                logger.error(
                    "Config value 'file_archive_path' must be set for the file distributor"
                )
                valid = False
            else:
                if not os.path.isdir(self.file_archive_path):
                    logger.error(
                        "Config value 'file_archive_path' must point to an existing folder"
                    )
                    valid = False

        return valid

# END Class Config

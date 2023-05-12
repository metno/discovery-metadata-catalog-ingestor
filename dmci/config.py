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
        self.pkg_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir))

        # Core Values
        self.call_distributors = []
        self.distributor_cache = None
        self.rejected_jobs_path = None
        self.max_permitted_size = 100000  # Size of files permitted through API
        self.mmd_xsl_path = None
        self.mmd_xsd_path = None
        self.path_to_parent_list = None

        # PyCSW Distributor
        self.csw_service_url = None

        # Environment-dependent web catalog url
        self.catalog_url = None
        # Environment-dependent suffix
        self.env_string = None

        # File Distributor
        self.file_archive_path = None

        # SolR Distributor
        self.solr_service_url = None
        self.solr_username = None
        self.solr_password = None
        self.authentication = None
        self.fail_on_missing_parent = True
        self.commit_on_delete = False

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
            logger.error("Config file not found: %s", configFile)
            return False

        try:
            with open(configFile, mode="r", encoding="utf8") as inFile:
                self._raw_conf = yaml.safe_load(inFile)
            logger.debug("Read config from: %s", configFile)
        except Exception as e:
            logger.error("Could not read file: %s", configFile)
            logger.error(str(e))
            return False

        # Read Values
        self._read_core()
        self._read_pycsw()
        self._read_customization()
        self._read_file()
        self._read_solr()

        # Read SolR environment credential variables
        self._read_solr_env_vars()

        valid = self._validate_config()

        return valid

    ##
    #  Internal Functions
    ##

    def _read_core(self):
        """Read config values under 'dmci'."""
        conf = self._raw_conf.get("dmci", {})

        self.call_distributors = conf.get(
            "distributors", self.call_distributors)
        self.distributor_cache = conf.get(
            "distributor_cache", self.distributor_cache)
        self.rejected_jobs_path = conf.get(
            "rejected_jobs_path", self.rejected_jobs_path)
        self.max_permitted_size = conf.get(
            "max_permitted_size", self.max_permitted_size)
        self.mmd_xsl_path = conf.get("mmd_xsl_path", self.mmd_xsl_path)
        self.mmd_xsd_path = conf.get("mmd_xsd_path", self.mmd_xsd_path)
        self.path_to_parent_list = conf.get(
            "path_to_parent_list", self.path_to_parent_list)

        return

    def _read_pycsw(self):
        """Read config values under 'pycsw'."""
        conf = self._raw_conf.get("pycsw", {})

        self.csw_service_url = conf.get(
            "csw_service_url", self.csw_service_url)

        return

    def _read_solr(self):
        """Read config values under 'solr'."""
        conf = self._raw_conf.get("solr", {})
        self.solr_service_url = conf.get(
            "solr_service_url", self.solr_service_url)
        self.fail_on_missing_parent = conf.get(
            "missing_parent_fail", self.fail_on_missing_parent)
        self.commit_on_delete = conf.get(
            "commit_on_delete", self.commit_on_delete)
        self.solr_username = conf.get("solr_username", self.solr_username)
        self.solr_password = conf.get("solr_password", self.solr_password)

        return

    def _read_solr_env_vars(self):
        """Read solr credentials from environment variables."""
        self.solr_username = os.getenv("SOLR_USERNAME", self.solr_username)
        self.solr_password = os.getenv("SOLR_PASSWORD", self.solr_password)

    def _read_customization(self):
        """Read config values under 'customization'."""
        conf = self._raw_conf.get("customization", {})

        self.catalog_url = conf.get("catalog_url", self.catalog_url)
        self.env_string = conf.get("env_string", self.env_string)

        return

    def _read_file(self):
        """Read config values under 'pycsw'."""
        conf = self._raw_conf.get("file", {})

        self.file_archive_path = conf.get(
            "file_archive_path", self.file_archive_path)

        return

    def _validate_config(self):
        """Check config variable dependencies. It needs to be called
        after all the read functions when all settings have been
        handled.
        """
        valid = True

        valid &= self._check_file_exists(self.mmd_xsd_path, "mmd_xsd_path")
        valid &= self._check_folder_exists(
            self.distributor_cache, "distributor_cache")
        valid &= self._check_folder_exists(
            self.rejected_jobs_path, "rejected_jobs_path")
        valid &= self._check_file_exists(
            self.path_to_parent_list, "path_to_parent_list")

        if "pycsw" in self.call_distributors:
            valid &= self._check_file_exists(self.mmd_xsl_path, "mmd_xsl_path")

        if "file" in self.call_distributors:
            valid &= self._check_folder_exists(
                self.file_archive_path, "file_archive_path")

        return valid

    def _check_file_exists(self, path, setting):
        """Check if a file exists, and if not report error."""
        if not isinstance(path, str):
            logger.error("Config value '%s' must be set", setting)
            return False
        if not os.path.isfile(path):
            logger.error(
                "Config value '%s' must point to an existing file", setting)
            return False
        return True

    def _check_folder_exists(self, path, setting):
        """Check if a folder exists, and if not report error."""
        if not isinstance(path, str):
            logger.error("Config value '%s' must be set", setting)
            return False
        if not os.path.isdir(path):
            logger.error(
                "Config value '%s' must point to an existing folder", setting)
            return False
        return True

# END Class Config

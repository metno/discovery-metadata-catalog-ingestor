# -*- coding: utf-8 -*-
"""
DMCI : Worker Class
===================

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

import logging

from lxml import etree

from dmci import CONFIG
from dmci.mmd_tools import full_check
from dmci.distributors import FileDist, PyCSWDist

logger = logging.getLogger(__name__)

class Worker():

    CALL_MAP = {
        "file": FileDist,
        "pycsw": PyCSWDist,
    }

    def __init__(self, xml_file, xsd_validator, **kwargs):

        self._conf = CONFIG

        # These should be populated with proper values to send to the
        # distributors

        self._dist_cmd = "insert"
        self._dist_xml_file = xml_file
        self._dist_metadata_id = None
        self._kwargs = kwargs
        self._file_metadata_id = None

        # XML
        self._xsd_obj = xsd_validator

        return

    ##
    #  Methods
    ##

    def validate(self, data):
        """Validate the xml file against the XML style definition,
        then check the information content.

        Input
        =====
        data : bytes
            bytes representation of the xml data

        Returns
        =======
        valid : boolean
            True if xsd and information content checks are passing
        msg : str
            Validation message
        """
        # Takes in bytes-object data
        # Gives msg when both validating and not validating
        if not isinstance(data, bytes):
            return False, "input must be bytes type"

        # Check xml file against XML schema definition
        valid = self._xsd_obj.validate(etree.fromstring(data))
        msg = self._xsd_obj.error_log
        if valid:
            # Check information content
            valid, msg = self._check_information_content(data)

        return valid, msg

    def distribute(self):
        """Loop through all distributors listed in the config and call
        them in the same order.
        """
        status = True
        valid  = True
        called = []
        failed = []
        skipped = []

        for dist in self._conf.call_distributors:
            if dist not in self.CALL_MAP:
                skipped.append(dist)
                continue

            obj = self.CALL_MAP[dist](
                self._dist_cmd,
                xml_file=self._dist_xml_file,
                metadata_id=self._dist_metadata_id,
                worker=self,
                **self._kwargs
            )
            valid &= obj.is_valid()
            if obj.is_valid:
                obj_status = obj.run()
                status &= obj_status
                if obj_status:
                    called.append(dist)
                else:
                    failed.append(dist)

        return status, valid, called, failed, skipped

    ##
    #  Internal Functions
    ##

    def _check_information_content(self, data):
        """Check the information content in the submitted file
        """
        if not isinstance(data, bytes):
            return False, "input must be bytes type"

        # Read XML file
        xml_doc = etree.fromstring(data)
        self._extract_metadata_id(xml_doc)

        # Check XML file
        logger.info("Performing in depth checking.")
        valid = full_check(xml_doc)
        if valid:
            msg = "Input MMD xml file is ok"
        else:
            msg = (
                "Input MMD xml file contains errors - please check your file "
                "(see https://github.com/metno/py-mmd-tools/blob/master/script/check_MMD)"
            )

        return valid, msg

    def _extract_metadata_id(self, xml_doc):
        """Extract the metadata_identifier from the xml object and set
        the class variable.
        """
        self._file_metadata_id = None
        for xml_entry in xml_doc:
            local = etree.QName(xml_entry)
            if local.localname == "metadata_identifier":
                self._file_metadata_id = xml_entry.text
                logger.info("XML file metadata_identifier: %s" % self._file_metadata_id)
                break

        if self._file_metadata_id is None:
            logger.warning("No metadata_identifier found in XML file")

        return

# END Class Worker

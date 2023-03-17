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

import uuid
import logging
import re
import sys

from lxml import etree

from dmci import CONFIG
from dmci.tools import CheckMMD
from dmci.distributors import FileDist, PyCSWDist

logger = logging.getLogger(__name__)


class Worker():

    CALL_MAP = {
        "file": FileDist,
        "pycsw": PyCSWDist,
    }

    def __init__(self, cmd, xml_file, xsd_validator, **kwargs):

        self._conf = CONFIG

        self._dist_cmd = None
        self._dist_xml_file = xml_file
        if cmd in ("insert", "update", "delete"):
            self._dist_cmd = cmd

        self._kwargs = kwargs
        self._dist_metadata_id = kwargs.get("md_uuid", None)
        self._namespace = kwargs.get("md_namespace", "")

        self._file_metadata_id = None

        # XML Validator
        # Created by the app object as it is potentially slow to set up
        self._xsd_obj = xsd_validator

        return

    ##
    #  Methods
    ##

    def validate(self, data):
        """Validate the xml file against the XML style definition,
        then check the information content.

        Parameters
        ----------
        data : bytes
            bytes representation of the xml data

        Returns
        -------
        valid : bool
            True if xsd and information content checks are passing
        msg : str
            Validation message
        """
        # Takes in bytes-object data
        # Gives msg when both validating and not validating
        if not isinstance(data, bytes):
            return False, "Input must be bytes type"

        # Check xml file against XML schema definition
        valid = self._xsd_obj.validate(etree.fromstring(data))
        msg = repr(self._xsd_obj.error_log)
        data_mod = data
        if valid:
            # Check information content
            valid, msg = self._check_information_content(data)
            data_mod = self._add_landing_page(data,self._conf.catalog_url)

        return valid, msg, data_mod

    def distribute(self):
        """Loop through all distributors listed in the config and call
        them in the same order.

        Returns
        -------
        status : bool
            True if all distributors ran ok
        valid : bool
            True if all distributors are valid
        called : list of str
            The names of all distributors that were called (status True)
        failed : list of str
            The names of all distributors that failed (status False)
        skipped : list of str
            The names of all distributors that were skipped (invalid)
        failed_msg : list of str
            The messages returned by the failed jobs
        """
        status = True
        valid  = True
        called = []
        failed = []
        skipped = []
        failed_msg = []

        for dist in self._conf.call_distributors:
            if dist not in self.CALL_MAP:
                skipped.append(dist)
                continue
            obj = self.CALL_MAP[dist](
                self._dist_cmd,
                xml_file=self._dist_xml_file,
                metadata_id=self._dist_metadata_id,
                worker=self,
                path_to_parent_list=self._kwargs.get('path_to_parent_list', None)
            )
            valid &= obj.is_valid()
            if obj.is_valid():
                obj_status, obj_msg = obj.run()
                status &= obj_status
                if obj_status:
                    called.append(dist)
                else:
                    failed.append(dist)
                    failed_msg.append(obj_msg)
            else:
                skipped.append(dist)

        return status, valid, called, failed, skipped, failed_msg

    ##
    #  Internal Functions
    ##

    def _check_information_content(self, data):
        """Check the information content in the submitted file."""
        if not isinstance(data, bytes):
            return False, "Input must be bytes type"

        # Read XML file
        xml_doc = etree.fromstring(data)
        valid = self._extract_metadata_id(xml_doc)
        if not valid:
            return False, "Input MMD XML file has no valid uri:UUID metadata_identifier"

        # Check XML file
        logger.info("Performing in depth checking.")
        checker = CheckMMD()
        valid = checker.full_check(xml_doc)
        if valid:
            msg = "Input MMD XML file is ok"
        else:
            _, _, err = checker.status()
            msg = "Input MMD XML file contains errors, please check your file"
            if err:
                msg += "\n" + "\n".join(err)

        return valid, msg

    def _add_landing_page(self, data, catalog_url):
        """Inserts the landing page info in the data bytes string and returns the modified string
        <related_information>
           <type>Dataset landing page</type>
           <resource>https://data.met.no/dataset/{uuid}</resource>
        </related_information>"""

        if not isinstance(data, bytes):
            #return False, "Input must be bytes type"
            logger.critical("Input must be bytes type")
            sys.exit(1)

        #extract uuid
        uuid=''
        try: 
            match_meta_id=re.search(b"metadata_identifier>(.+?)</mmd:metadata_identifier>", data)
            meta_id = match_meta_id.group(1)
            uuid = meta_id.split(b":")[1].decode()
        except Exception as e:
            logger.critical("The data should have been validated and contain a valid metadata_identifier")
            logger.critical(str(e))
            sys.exit(1)

        # each of the related_information types has its own block so we do not eed to worry about there being other <mmd:related_information> </mmd:related_information> blocks already, unless it's a Dataset Landing Page block
        if not bool(re.search(b"Dataset landing page",data)):
            matchstring_end = b"\n</mmd:mmd>\n"
            end_mod = str.encode(f"\n  <mmd:related_information>\n    <mmd:type>Dataset landing page</mmd:type>\n    <mmd:description/>\n    <mmd:resource>{catalog_url}/{uuid}</mmd:resource>\n  </mmd:related_information>\n</mmd:mmd>\n")
            data_mod = re.sub(matchstring_end,end_mod,data)
        else: # there is already a block of related information with Dataset landing page, we replace the content
            match_datasetlandingpage=re.search(b"<mmd:type>Dataset landing page</mmd:type>(.+?)</mmd:related_information>",data,re.DOTALL)
            found_datasetlandingpage = match_datasetlandingpage.group(1)
            datasetlandingpage_mod = str.encode(f"\n    <mmd:description/>\n    <mmd:resource>{catalog_url}/{uuid}</mmd:resource>\n  ")
            data_mod = re.sub(found_datasetlandingpage,datasetlandingpage_mod,data)

        return data_mod


    def _extract_metadata_id(self, xml_doc):
        """Extract the metadata_identifier from the xml object and set
        the class variables namespace and file_metadata_id.

        Returns
        -------
        status : bool
            True if both namespace and metadata_id is found, False
            if either is missing, or if metadata_id can not be cast
            as UUID type
        """
        self._file_metadata_id = None
        self._namespace = None
        file_uuid = ""
        namespace = ""
        for xml_entry in xml_doc:
            local = etree.QName(xml_entry)
            if local.localname == "metadata_identifier":
                # only accept if format is uri:UUID, both need to be present
                words = xml_entry.text.split(":")
                if len(words) != 2:
                    logger.warning("metadata_identifier not formed as namespace:UUID")
                    return False
                namespace, file_uuid = words

                logger.info("XML file metadata_identifier namespace:%s", namespace)
                logger.info("XML file metadata_identifier UUID: %s", file_uuid)
                break

        if file_uuid == "":
            logger.warning("No UUID found in XML file")
            return False
        if namespace == "":
            logger.warning("No namespace found in XML file")
            return False

        try:
            self._file_metadata_id = uuid.UUID(file_uuid)
            logger.debug("File UUID: %s", str(file_uuid))
        except Exception as e:
            logger.error("Could not parse UUID: '%s'", str(file_uuid))
            logger.error(str(e))
            return False
        self._namespace = namespace
        return True

# END Class Worker

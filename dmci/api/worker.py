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
import re
import uuid

from lxml import etree

from dmci import CONFIG
from dmci.distributors import FileDist, PyCSWDist
from dmci.tools import CheckMMD

logger = logging.getLogger(__name__)


class Worker:

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
        self._file_title_en = None

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

        if valid:
            # Check information content
            valid, msg = self._check_information_content(data)

            # Check if parent dataset block is present and if yes
            # check that the parent exists in the databases
            if bool(re.search(b'<mmd:related_dataset relation_type="parent">', data)):
                match_parent_block = re.search(
                    b'<mmd:related_dataset relation_type="parent">(.+?)</mmd:related_dataset>',
                    data
                )
                for dist in self.CALL_MAP:
                    obj = self.CALL_MAP[dist](self._dist_cmd)
                found_parent_block_content = match_parent_block.group(1)
                old_parent_namespace, parent_uuid = [x.decode()
                                                     for x in
                                                     found_parent_block_content.split(b":")]
                found_parent = obj.search(parent_uuid)
                if not found_parent:
                    return False, "Parent uuid not found"
                if self._conf.env_string:
                    # Append env string to the namespace in the parent block
                    new_parent_namespace = f"{old_parent_namespace}.{self._conf.env_string}"
                    new_parent_block_content = bytes(
                        f"{new_parent_namespace}:{parent_uuid}", "utf-8")
                    data = re.sub(found_parent_block_content, new_parent_block_content, data)

            # Append env string to namespace in metadata_identifier
            if self._conf.env_string:
                full_namespace = f"{self._namespace}.{self._conf.env_string}"
                data = re.sub(
                    str.encode(f"<mmd:metadata_identifier>{self._namespace}"),
                    str.encode(f"<mmd:metadata_identifier>{full_namespace}"),
                    data,
                )
                self._namespace = full_namespace

            # Add landing page info
            data = self._add_landing_page(
                data, self._conf.catalog_url, self._file_metadata_id
            )

        return valid, msg, data

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
        valid = True
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
                path_to_parent_list=self._kwargs.get("path_to_parent_list", None),
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
        self._extract_title(xml_doc)
        valid = self._extract_metadata_id(xml_doc)
        if not valid:
            return (
                False,
                f"Input MMD XML file has no valid uri:UUID metadata_identifier \n"
                f" Title: {self._file_title_en} ",
            )

        # Check XML file
        logger.info("Performing in depth checking.")
        checker = CheckMMD()
        valid = checker.full_check(xml_doc)
        if valid:
            msg = "Input MMD XML file is ok"
        else:
            _, _, err = checker.status()
            msg = (
                f"Input MMD XML file contains errors, please check your file.\n"
                f" Title: {self._file_title_en}"
            )
            if err:
                msg += "\n" + "\n".join(err)

        return valid, msg

    def _add_landing_page(self, data, catalog_url, uuid):
        """Inserts the landing page info in the data bytes string and returns the modified string
        <related_information>
           <type>Dataset landing page</type>
           <resource>https://data.met.no/dataset/{uuid}</resource>
        </related_information>"""

        # each of the related_information types has its own block so we do not eed to worry
        # about there being other <mmd:related_information> </mmd:related_information> blocks
        # already, unless it's a Dataset Landing Page block
        if not bool(re.search(b"Dataset landing page", data)):
            matchstring_end = b"\n</mmd:mmd>\n"
            end_mod = str.encode(
                f"\n  <mmd:related_information>\n    <mmd:type>Dataset landing page</mmd:type>"
                f"\n    <mmd:description/>\n    <mmd:resource>{catalog_url}/{uuid}</mmd:resource>"
                f"\n  </mmd:related_information>\n</mmd:mmd>\n"
            )
            data_mod = re.sub(matchstring_end, end_mod, data)
        else:
            # there is already a block of related_information with Dataset landing page,
            # we replace whatever the content inside it
            match_datasetlandingpage = re.search(
                b"<mmd:type>Dataset landing page</mmd:type>(.+?)</mmd:related_information>",
                data,
                re.DOTALL,
            )
            found_datasetlandingpage = match_datasetlandingpage.group(1)
            datasetlandingpage_mod = str.encode(
                f"\n    <mmd:description/>\n    "
                f"<mmd:resource>{catalog_url}/{uuid}</mmd:resource>\n  "
            )
            data_mod = re.sub(found_datasetlandingpage, datasetlandingpage_mod, data)

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

    def _extract_title(self, xml_doc):
        title = ""
        for xml_entry in xml_doc:
            local = etree.QName(xml_entry)
            if local.localname == "title":
                title = xml_entry.text
                logger.info("XML file title:%s", title)
                break
        if title == "":
            logger.warning("No title found in XML file")
        self._file_title_en = title


# END Class Worker

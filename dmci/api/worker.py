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
from dmci.distributors import FileDist, PyCSWDist, SolRDist
from dmci.tools import CheckMMD

logger = logging.getLogger(__name__)


class Worker:

    CALL_MAP = {"file": FileDist, "pycsw": PyCSWDist, "solr": SolRDist}

    def __init__(self, cmd, xml_file, xsd_validator, **kwargs):

        self._conf = CONFIG

        self._dist_cmd = None
        self._dist_xml_file = xml_file
        if cmd in ("insert", "update", "delete"):
            self._dist_cmd = cmd

        self._kwargs = kwargs

        # ID as given from API
        self._dist_metadata_id_uuid = kwargs.get("md_uuid", None)
        self._namespace = kwargs.get("md_namespace", "")
        # Metadata ID read from file given as input to the API
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
        data : bytes
            bytes representation of the xml data
        """
        # Takes in bytes-object data
        # Gives msg when both validating and not validating
        valid = False
        msg = ""
        if not isinstance(data, bytes):
            return False, "Input must be bytes type", data

        # Check xml file against XML schema definition
        try:
            valid = self._xsd_obj.validate(etree.fromstring(data))
            msg = repr(self._xsd_obj.error_log)
        except Exception as e:
            return False, str(e), data

        if valid:
            # Check information content
            valid, msg = self._check_information_content(data)
            if not valid:
                return valid, msg, data

            # Make sure that datasets in dev (e.g., no.met.dev) and
            # staging (e.g., no.met.staging) cannot be added to wrong
            # environments
            if (".dev" in self._namespace and self._conf.env_string != "dev") or (
                ".staging" in self._namespace and self._conf.env_string != "staging"
            ):
                msg = (
                    f"Namespace {self._namespace} does not match "
                    f"the env {self._conf.env_string}"
                )
                return False, msg, data

            if self._conf.env_string:

                # Append env string to namespace in metadata_identifier
                logger.debug("Identifier namespace: %s" % self._namespace)
                logger.debug("Environment customization: %s" % self._conf.env_string)
                ns_re_pattern = re.compile(r"\w.\w." + self._conf.env_string)

                if re.search(ns_re_pattern, self._namespace) is None:
                    full_namespace = f"{self._namespace}.{self._conf.env_string}"
                    data = re.sub(
                        str.encode(f"<mmd:metadata_identifier>{self._namespace}"),
                        str.encode(f"<mmd:metadata_identifier>{full_namespace}"),
                        data,
                    )
                    self._namespace = full_namespace

                # Append env string to the namespace in the parent block, if present
                if bool(
                    re.search(b'<mmd:related_dataset relation_type="parent">', data)
                ):
                    match_parent_block = re.search(
                        b'<mmd:related_dataset relation_type="parent">(.+?)</mmd:related_dataset>',
                        data,
                    )
                    found_parent_block_content = match_parent_block.group(1)
                    found_parent_block_content = found_parent_block_content.split(b":")
                    if len(found_parent_block_content) != 2:
                        err = f"Malformed parent dataset identifier {found_parent_block_content}"
                        logger.error(err)
                        return False, err, data
                    old_parent_namespace = found_parent_block_content[0].decode()
                    logger.debug("Parent dataset namespace: %s" % old_parent_namespace)
                    if re.search(ns_re_pattern, old_parent_namespace) is None:
                        new_parent_namespace = (
                            f"{old_parent_namespace}.{self._conf.env_string}"
                        )
                        data = re.sub(
                            str.encode(
                                f"<mmd:related_dataset "
                                f'relation_type="parent">{old_parent_namespace}'
                            ),
                            str.encode(
                                f"<mmd:related_dataset "
                                f'relation_type="parent">{new_parent_namespace}'
                            ),
                            data,
                        )

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
                metadata_UUID=self._dist_metadata_id_uuid,
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
                f"\n    <mmd:description/>"
                f"\n    <mmd:resource>{catalog_url}/dataset/{uuid}</mmd:resource>"
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
                f"<mmd:resource>{catalog_url}/dataset/{uuid}</mmd:resource>\n  "
            )
            data_mod = re.sub(found_datasetlandingpage, datasetlandingpage_mod, data)

        return data_mod

    def _extract_metadata_id(self, xml_doc):
        """Set the class variables namespace and file_metadata_id.

        Returns
        -------
        status : bool
            True if both namespace and metadata_id is found, False
            if either is missing, or if metadata_id can not be cast
            as UUID type
        """
        self._file_metadata_id = None
        self._namespace = None
        namespace, file_uuid = self._get_metadata_id(xml_doc)
        if file_uuid == "":
            logger.error("No UUID found in XML file")
            return False
        if namespace == "":
            logger.error("No namespace found in XML file")
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

    @staticmethod
    def _get_metadata_id(xml_doc):
        """Extract the metadata_identifier from the xml object.

        Returns
        -------
        namespace : str
            Namespace if this is found, otherwise empty string
        file_uuid : str
            File UUID if this is found, otherwise empty string
        """
        file_uuid = ""
        namespace = ""
        for xml_entry in xml_doc:
            local = etree.QName(xml_entry)
            if local.localname == "metadata_identifier":
                # only accept if format is uri:UUID, both need to be present
                words = xml_entry.text.split(":")
                if len(words) != 2:
                    logger.error("metadata_identifier not formed as namespace:UUID")
                    return "", ""
                namespace, file_uuid = words

                logger.info(
                    "XML file metadata_identifier: %s:%s" % (namespace, file_uuid)
                )
                logger.debug("XML file metadata_identifier namespace: %s", namespace)
                logger.debug("XML file metadata_identifier UUID: %s", file_uuid)
                break
        return namespace, file_uuid

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

"""
DMCI : PyCSW Distributor
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

import uuid
import logging
import requests

from lxml import etree

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)


class PyCSWDist(Distributor):

    TOTAL_DELETED = "total_deleted"
    TOTAL_INSERTED = "total_inserted"
    TOTAL_UPDATED = "total_updated"
    STATUS = [TOTAL_DELETED, TOTAL_INSERTED, TOTAL_UPDATED]

    def __init__(self, cmd, xml_file=None, metadata_UUID=None, worker=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_UUID, worker, **kwargs)
        return

    def run(self):
        """Run function to handle insert, update or delete

        Returns
        -------
        status : bool
            True if successful insert, update or delete
            False if not
        """
        status = False
        msg = "No job was run"
        if not self.is_valid():
            return False, "The run job is invalid"

        if self._cmd == DistCmd.UPDATE:
            status, msg = self._update()
        elif self._cmd == DistCmd.DELETE:
            status, msg = self._delete()
        elif self._cmd == DistCmd.INSERT:
            status, msg = self._insert()

        return status, msg

    def _translate(self):
        """Convert from MMD to ISO19139, Norwegian INSPIRE profile."""
        result = b""
        try:
            xml_doc = etree.ElementTree(file=self._xml_file)
            transform = etree.XSLT(etree.parse(self._conf.mmd_xsl_path))
            # If the dataset is a parent dataset, the
            # self._xml_file needs to contain the string "parent"
            new_doc = transform(xml_doc, path_to_parent_list=etree.XSLT.strparam(
                self._path_to_parent_list))
            result = etree.tostring(new_doc, pretty_print=False, encoding="utf-8")
        except Exception as e:
            logger.error("Failed to translate MMD to ISO19139")
            logger.debug(str(e))

        return b"" if result is None else result

    def _insert(self):
        """Insert in pyCSW using a Transaction."""
        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        xml = (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<csw:Transaction xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
            b'xmlns:ows="http://www.opengis.net/ows" '
            b'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            b'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
            b'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" '
            b'service="CSW" version="2.0.2"><csw:Insert>'
        )
        xml += self._translate()
        xml += b"</csw:Insert></csw:Transaction>"
        return self._post_request(headers, xml, "insert", self.TOTAL_INSERTED)

    def _update(self):
        """Update current entry.

        Note: in pycsw updates are made on elements, not full documents. The
        way dmci is designed, it is easier to update the full document by first
        deleting the current entry, then inserting the new version.
        """
        from dmci.api.worker import Worker
        with open(self._xml_file) as fn:
            data = fn.read()
        xml_doc = etree.fromstring(data)
        namespace, file_uuid = Worker._get_metadata_id(xml_doc)
        if file_uuid == "":
            return False, "No UUID found in XML file"
        if namespace == "":
            return False, "No namespace found in XML file"
        try:
            self._metadata_UUID = uuid.UUID(file_uuid)
            logger.debug("File UUID: %s", str(file_uuid))
        except Exception as e:
            logger.error(str(e))
            return False, f"Could not parse UUID: {str(file_uuid)}"

        del_status, del_response_text = self._delete()
        if not del_status:
            return del_status, del_response_text
        ins_status, ins_response_text = self._insert()
        if not ins_status:
            return ins_status, ins_response_text
        else:
            response_text = ins_response_text.replace("insert", "update")
        # Handle insertion
        return ins_status, response_text

    def _delete(self):
        """Delete entry with a specified metadata_id."""
        identifier = self._construct_identifier(self._worker._namespace, self._metadata_UUID)
        logger.debug(f"Deleting file: {identifier}")

        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        xml_as_string = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<csw:Transaction xmlns:ogc="http://www.opengis.net/ogc" '
            'xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
            'xmlns:ows="http://www.opengis.net/ows" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
            'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" '
            'service="CSW" version="2.0.2">'
            '  <csw:Delete>'
            '    <csw:Constraint version="1.1.0">'
            '      <ogc:Filter>'
            '        <ogc:PropertyIsEqualTo>'
            '          <ogc:PropertyName>apiso:Identifier</ogc:PropertyName>'
            '          <ogc:Literal>%s</ogc:Literal>'
            '        </ogc:PropertyIsEqualTo>'
            '      </ogc:Filter>'
            '    </csw:Constraint>'
            '  </csw:Delete>'
            '</csw:Transaction>'
        ) % identifier
        return self._post_request(headers, xml_as_string, "delete", self.TOTAL_DELETED)

    def _get_transaction_status(self, key, resp):
        """Check response status, read response text, and get status.

        Parameters
        ----------
        key : str
            total_inserted, total_updated or total_deleted
        resp : requests.models.Response
            Response object from transaction

        Returns
        -------
        bool
            True if the transaction succeeded, otherwise False
        """
        if key not in self.STATUS:
            logger.error("Input key must be one of: %s", ", ".join(self.STATUS))
            return False

        if not isinstance(resp, requests.models.Response):
            logger.error("Input argument 'resp' must be instance of requests.models.Response")
            return False

        status = False
        if resp.status_code >= 200 and resp.status_code < 300:
            status = self._read_response_text(key, resp.text)
        else:
            logger.error(resp.text)

        return status

    def _read_response_text(self, key, text):
        """Read response xml text and return a dict with results.

        Parameters
        ----------
        key : str
            total_inserted, total_updated or total_deleted
        text : str
            xml representation of the pycsw result

        Returns
        -------
        dict
            status of insert, update and delete
        """
        if key not in self.STATUS:
            logger.error("Input key must be one of: %s", ", ".join(self.STATUS))
            return False

        status = False
        try:
            root = etree.fromstring(text.encode("utf-8").strip())
        except Exception as e:
            logger.error("Could not parse response XML from PyCSW")
            logger.debug(str(e))
            return status

        ns_ows = root.nsmap.get("ows", "")
        ns_csw = root.nsmap.get("csw", "")

        n_ins = "0"
        n_upd = "0"
        n_del = "0"

        if root.tag == "{%s}ExceptionReport" % ns_ows:
            node = root.find("{%s}Exception" % ns_ows, root.nsmap)
            msg = "Unknown Error"
            if node is not None:
                msg = node.findtext("{%s}ExceptionText" % ns_ows, "Unknown Error", root.nsmap)
            else:
                msg = "Unknown Error"
            logger.error(msg)

        elif root.tag == "{%s}TransactionResponse" % ns_csw:
            node = root.find("{%s}TransactionSummary" % ns_csw, root.nsmap)
            if node is not None:
                n_ins = node.findtext("{%s}totalInserted" % ns_csw, "0", root.nsmap)
                n_upd = node.findtext("{%s}totalUpdated" % ns_csw, "0", root.nsmap)
                n_del = node.findtext("{%s}totalDeleted" % ns_csw, "0", root.nsmap)

        else:
            msg = "This should not happen"
            logger.error(msg)

        res_dict = {
            self.TOTAL_INSERTED: n_ins,
            self.TOTAL_UPDATED: n_upd,
            self.TOTAL_DELETED: n_del,
        }

        # In principle, we can insert, update or delete multiple datasets
        if int(res_dict[key]) >= 1:
            status = True

        return status

    def _post_request(self, headers, xml, cmd, key):
        """Send an HTTP POST request to pyCSW using a Transaction.

        Parameters
        ----------
        headers : dict
            Headers to be included in the request.
        xml : str
            XML data to be sent in the request body.
        cmd : str
            The command being executed.
        key : str
            Key specifying the type of transaction: 'total_inserted', 'total_updated',
            or 'total_deleted'.

        Returns
        -------
        tuple of (bool, str)
            A tuple containing a boolean indicating whether the transaction succeeded (True) or
            failed (False), and a string providing additional information about the transaction
            status or error message.
        """
        try:
            resp = requests.post(self._conf.csw_service_url, headers=headers, data=xml)
        except Exception as e:
            logger.error(str(e))
            return False, (
                "%s: service unavailable. Failed to %s." %
                (self._conf.csw_service_url, cmd)
            )
        status = self._get_transaction_status(key, resp)
        logger.debug(cmd + " status: " + str(status) + ". With response: " + resp.text)
        return status, resp.text

# END Class PyCSWDist

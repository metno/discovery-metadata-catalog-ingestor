# -*- coding: utf-8 -*-
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

import logging
import requests

from lxml import etree

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)

class PyCSWDist(Distributor):

    TOTAL_DELETED = 'total_deleted'
    TOTAL_INSERTED = 'total_inserted'
    TOTAL_UPDATED = 'total_updated'
    STATUS = [TOTAL_DELETED, TOTAL_INSERTED, TOTAL_UPDATED]

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, **kwargs)
        return

    def run(self):
        """Run function to handle insert, update or delete

        Returns
        =======
        status : bool
            True if successful insert, update or delete
            False if not
        """
        if not self.is_valid():
            return False

        status = False
        if self._cmd == DistCmd.UPDATE:
            status = self._update()
        elif self._cmd == DistCmd.DELETE:
            status = self._delete()
        elif self._cmd == DistCmd.INSERT:
            status = self._insert()
        else:
            logger.error('Invalid command: %s' % str(self._cmd)) # pragma: no cover

        return status

    def _translate(self):
        """Convert from MMD to ISO19139, Norwegian INSPIRE profile
        """
        pretty = ""
        try:
            xml_doc = etree.ElementTree(file=self._xml_file)
            transform = etree.XSLT(etree.parse(self._conf.mmd_xslt_path))
            new_doc = transform(xml_doc)
            pretty = etree.tostring(new_doc, pretty_print=True, encoding="utf-8")
        except Exception as e:
            logger.error("Failed to translate MMD to ISO19139")
            logger.debug(str(e))

        return pretty

    def _insert(self):
        """Insert in pyCSW using a Transaction
        """
        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        xml_as_string = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<csw:Transaction xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
            'xmlns:ows="http://www.opengis.net/ows" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
            'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" '
            'service="CSW" version="2.0.2">'
            '    <csw:Insert>%s</csw:Insert>'
            '</csw:Transaction>'
        ) % self._translate()
        resp = requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)
        status = self._get_transaction_status(self.TOTAL_INSERTED, resp)

        return status

    def _update(self):
        """Update current entry

        Need to find out how to do this...
        """
        logger.warning("Not yet implemented")

        return False

    def _delete(self):
        """
        """
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
            '   <csw:Delete>'
            '       <csw:Constraint version="1.1.0">'
            '           <ogc:Filter>'
            '               <ogc:PropertyIsEqualTo>'
            '                   <ogc:PropertyName>apiso:Identifier</ogc:PropertyName>'
            '                   <ogc:Literal>%s</ogc:Literal>'
            '               </ogc:PropertyIsEqualTo>'
            '           </ogc:Filter>'
            '       </csw:Constraint>'
            '   </csw:Delete>'
            '</csw:Transaction>'
        ) % self._metadata_id
        resp = requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)
        status = self._get_transaction_status(self.TOTAL_DELETED, resp)

        return status

    def _get_transaction_status(self, key, resp):
        """Check response status, read response text, and get status (boolean)

        Input
        =====
        key : str
            total_inserted, total_updated or total_deleted
        resp : requests.models.Response
            Response object from transaction

        Return
        ======
        status : boolean
            True if the transaction succeeded, otherwise False
        """
        if key not in self.STATUS:
            logger.error("Input key must be '%s', '%s' or '%s'" % (
                self.TOTAL_INSERTED, self.TOTAL_UPDATED, self.TOTAL_DELETED)
            )
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
        """Read response xml text and return a dict with results

        Input
        =====
        text : str
            xml representation of the pycsw result

        Return
        ======
        dict : status of insert, update and delete
        """
        if key not in self.STATUS:
            logger.error("Input key must be '%s', '%s' or '%s'" % (
                self.TOTAL_INSERTED, self.TOTAL_UPDATED, self.TOTAL_DELETED)
            )
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

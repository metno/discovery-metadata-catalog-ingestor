import logging
import os
import requests
import xml

from py_mmd_tools.xml_utils import xml_translate

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)

class PyCSWDist(Distributor):

    TOTAL_DELETED = 'total_deleted'
    TOTAL_INSERTED = 'total_inserted'
    TOTAL_UPDATED = 'total_updated'
    STATUS = [TOTAL_DELETED, TOTAL_INSERTED, TOTAL_UPDATED]

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):

        super().__init__(cmd, xml_file, metadata_id, **kwargs)

    def run(self):

        Distributor.run(self)

        status = False
        msg = ""
        if self._cmd == DistCmd.UPDATE:
            status = self._update()
        elif self._cmd == DistCmd.DELETE:
            status = self._delete()
        elif self._cmd == DistCmd.INSERT:
            status = self._insert()
        else:
            logger.error('Invalid command: %s' %str(self._cmd)) # pragma: no cover

        return status

    def _translate(self, xslt):
        """ Convert from MMD to ISO19139, Norwegian INSPIRE profile """
        return xml_translate(self._xml_file, xslt)

    def _insert(self, xslt='../mmd/xslt/mmd-to-geonorge.xslt'):
        """ Insert in pyCSW using a Transaction 

        TODO:
                - DONE: configure pycsw service (Transactions are disabled by default; to enable, `manager.transactions` must be set to true. Access to transactional functionality is limited to IP addresses which must be set in `manager.allowed_ips`.)
                - DONE: insertion can now be done through met user networks
                - check how to do insert transaction in pycsw test suite (source code)

        """
        if self._xml_file is None:
            msg = "File does not exist: %s" % str(self._xml_file)
            logger.error(msg)
            return False
        iso = self._translate(xslt)

        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        header = """<?xml version="1.0" encoding="UTF-8"?><csw:Transaction xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:ows="http://www.opengis.net/ows" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" service="CSW" version="2.0.2"><csw:Insert>"""
        footer = """</csw:Insert></csw:Transaction>"""
        xml_as_string = header + iso + footer
        resp = requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)
        status = self.get_status(self.TOTAL_INSERTED, resp)

        return status

    def _update(self):
        """ Update current entry

        Need to find out how to do this...
        """
        logger.error("Not yet implemented")

        import pdb
        pdb.set_trace()
        retval = True

        return retval

    def _delete(self):
        if self._metadata_id is None:
            msg = "Metadata identifier must be a non-empty string"
            logger.error(msg)
            return False

        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        xml_as_string = """<?xml version="1.0" encoding="UTF-8"?>
        <csw:Transaction xmlns:ogc="http://www.opengis.net/ogc" xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:ows="http://www.opengis.net/ows" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" service="CSW" version="2.0.2">
          <csw:Delete>
            <csw:Constraint version="1.1.0">
              <ogc:Filter>
                <ogc:PropertyIsEqualTo>
                  <ogc:PropertyName>apiso:Identifier</ogc:PropertyName>
                  <ogc:Literal>%s</ogc:Literal>
                </ogc:PropertyIsEqualTo>
              </ogc:Filter>
            </csw:Constraint>
          </csw:Delete>
        </csw:Transaction>""" %self._metadata_id

        resp = requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)
        status = self.get_status(self.TOTAL_DELETED, resp)

        return status

    def get_status(self, key, resp):
        """ Check response status, read response text, and get status (boolean)

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
        if not key in self.STATUS:
            logger.error("Input key must be '%s', '%s' or '%s'" %(self.TOTAL_INSERTED, 
                self.TOTAL_UPDATED, self.TOTAL_DELETED))
            return False
        status = False
        if resp.status_code>=200 and resp.status_code<300:
            status = self.read_response_text(key, resp.text)
        else:
            logger.error(resp.text)

        return status


    def read_response_text(self, key, text):
        """ Read response xml text and return a dict with results

        Input
        =====
        text : str
            xml representation of the pycsw result

        Return
        ======
        dict : status of insert, update and delete
        """
        status = False
        dom = xml.dom.minidom.parseString(text.encode('utf-8').strip())
        # should only contain the standard_name_table:
        node0 = dom.childNodes[1]
        tag_name = node0.tagName
        n_ins = 0
        n_upd = 0
        n_del = 0
        if tag_name=='ows:ExceptionReport':
            msg = node0.getElementsByTagName('ows:ExceptionText')[0].childNodes[0].data
            logger.error(msg)
        elif tag_name=='csw:TransactionSummary':
            node = node0.getElementsByTagName(tag_name)[0]
            n_ins = node.getElementsByTagName('csw:totalInserted')[0].childNodes[0].data
            n_upd = node.getElementsByTagName('csw:totalUpdated')[0].childNodes[0].data
            n_del = node.getElementsByTagName('csw:totalDeleted')[0].childNodes[0].data
        else:
            msg = ""
            print(msg)
            logger.error(msg)
        res_dict = {
            self.TOTAL_INSERTED: n_ins,
            self.TOTAL_UPDATED: n_upd,
            self.TOTAL_DELETED: n_del,
        }
        if res_dict[key] == 1:
            status = True
        return status


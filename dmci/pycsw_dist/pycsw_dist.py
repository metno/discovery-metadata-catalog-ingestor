import logging
import os
import requests

from py_mmd_tools.xml_utils import xml_translate

from dmci.distributor import Distributor, DistCmd

class PyCSWDist(Distributor):

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):

        super().__init__(cmd, xml_file, metadata_identifier, **kwargs)

        self.mmd_file = mmd_file

    def run(self):

        Distributor.run(self)

        if self._cmd == distCmd.UPDATE:
            self._update()
        elif self._cmd == distCmd.DELETE:
            self._delete()
        elif self._cmd == distCmd.INSERT:
            self._insert()
        else:
            logger.error('Invalid command: %s' %str(self._cmd)) # pragma: no cover
            return False

        return True

    def _translate(self, xslt):
        """ Convert from MMD to ISO19139, Norwegian INSPIRE profile """
        return xml_translate(self.mmd_file, xslt)

    def _insert(self, xslt):
        """ Insert in pyCSW using a Transaction 

        TODO:
                - DONE: configure pycsw service (Transactions are disabled by default; to enable, `manager.transactions` must be set to true. Access to transactional functionality is limited to IP addresses which must be set in `manager.allowed_ips`.)
                - DONE: insertion can now be done through met user networks
                - check how to do insert transaction in pycsw test suite (source code)

        """
        iso = self.translate(xslt)

        headers = requests.structures.CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        headers["Accept"] = "application/xml"
        header = """<?xml version="1.0" encoding="UTF-8"?><csw:Transaction xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:ows="http://www.opengis.net/ows" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd" service="CSW" version="2.0.2"><csw:Insert>"""
        footer = """</csw:Insert></csw:Transaction>"""
        xml_as_string = header + iso + footer
        return requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)

    def _update(self):
        """ Update current entry

        Need to find out how to do this...
        """
        logger.error("Not yet implemented")
        return False

    def _delete(self, id):

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
        </csw:Transaction>""" %id
        return requests.post(self._conf.csw_service_url, headers=headers, data=xml_as_string)

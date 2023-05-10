"""
DMCI : SolR Distributor
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

from solrindexer.indexdata import MMD4SolR, IndexMMD
from requests.auth import HTTPBasicAuth

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)


class SolRDist(Distributor):

    TOTAL_DELETED = "total_deleted"
    TOTAL_INSERTED = "total_inserted"
    TOTAL_UPDATED = "total_updated"
    STATUS = [TOTAL_DELETED, TOTAL_INSERTED, TOTAL_UPDATED]

    def __init__(self, cmd, xml_file=None, metadata_id=None, worker=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, worker, **kwargs)

        """Store solr autentication credentials if used"""
        self.username = kwargs.get('username', None)
        self.password = kwargs.get('password', None)
        self.authentication = None

        if self.username is not None and self.password is not None:
            self.authentication = HTTPBasicAuth(self.username, self.password)

        """Create connection to solr"""
        self.mysolr = IndexMMD(self._conf.solr_service_url, False)
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
            status, msg = self._add()
        elif self._cmd == DistCmd.DELETE:
            status, msg = self._delete()
        elif self._cmd == DistCmd.INSERT:
            status, msg = self._add()

        return status, msg

    def _add(self):
        """Index to SolR."""
        try:
            mydoc = MMD4SolR(self._xml_file)
        except Exception as e:
            logger.error("Could not read file %s: %s", (self._xml_file, e))
            return False, e

        mydoc.check_mmd()  # Determine if we need to run this check.

        """ Convert mmd file to solr format """
        try:
            newdoc = mydoc.tosolr()
        except Exception as e:
            logger.error('Could not process the file %s: %s', (self._xml_file, e))
            return False, e

        """Check if document already exsists. Then we throw error and don't index."""
        isIndexed = self.mysolr.get_dataset(newdoc['id'])
        if isIndexed['doc'] is not None and self._cmd == DistCmd.INSERT:
            logger.error("Document already exists in index, %s" % newdoc['metadata_identifier'])
            return False, "Document already exists in index, %s" % newdoc['metadata_identifier']

        elif 'related_dataset' in newdoc:
            logger.info("Child/Level-2 - dataset - %s", newdoc['metadata_identifier'])
            # newdoc.update({'dataset_type': 'Level-2'})
            # newdoc.update({'isChild': True})
            logger.info("Child dataset with parent id %s", newdoc['related_dataset'])
            parentid = newdoc['related_dataset_id']
            status, msg = self.mysolr.update_parent(parentid)
            if status:
                try:
                    self.mysolr.index_record(newdoc, addThumbnail=False, level=2)
                except Exception as e:
                    logger.error("Could not index file %s: %s", (self._xml_file, e))
                    return False, e
            else:
                return status, msg
        else:
            logger.info("Parent/Level-1 - dataset - %s", newdoc['metadata_identifier'])
            # newdoc.update({'dataset_type':'Level-1'})
            # newdoc.update({'isParent': False})
            try:
                self.mysolr.index_record(newdoc, addThumbnail=False, level=1)
            except Exception as e:
                logger.error("Could not index file %s: %s", (self._xml_file, e))
                return False, e

        return True, ""

    def _delete(self):
        """Delete entry with a specified metadata_id."""
        identifier = self._construct_identifier(self._worker._namespace, self._metadata_id)
        logger.info("Deleting document %s from SolR index." % identifier)
        status, msg = self.mysolr.delete(identifier,commit=True)
        # return status, resp.text
        return status, msg

    @staticmethod
    def _construct_identifier(namespace, metadata_id):
        """Helper function to construct identifier from namespace and
        UUID. Currently accepts empty namespaces, but later will only
        accept correctly formed namespaced UUID

        Parameters
        ----------
        namespace : str
            namespace for the UUID
        metadata_id : UUID or str
            UUID for the metadata-file we want to Update or Delete

        Returns
        -------
        str
            namespace:UUID or just UUID if namespace is empty.
        """
        if namespace != "":
            identifier = namespace + ":" + str(metadata_id)
        else:
            identifier = str(metadata_id)
        return identifier

# END Class PyCSWDist

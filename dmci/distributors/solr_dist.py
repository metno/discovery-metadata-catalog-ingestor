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

"""Configure log level for solrindexer
TODO: Maybe read this from env variable SOLRINDEXER_LOGLEVEL?
"""
logging.getLogger('solrindexer').setLevel(logging.WARNING)


class SolRDist(Distributor):

    TOTAL_DELETED = "total_deleted"
    TOTAL_INSERTED = "total_inserted"
    TOTAL_UPDATED = "total_updated"
    STATUS = [TOTAL_DELETED, TOTAL_INSERTED, TOTAL_UPDATED]

    def __init__(self, cmd, xml_file=None, metadata_id=None, worker=None, **kwargs):

        super().__init__(cmd, xml_file, metadata_id, worker, **kwargs)

        self.authentication = self._init_authentication()

        #  Create connection to solr
        self.mysolr = IndexMMD(self._conf.solr_service_url, always_commit=False,
                               authentication=self.authentication)
        return

    def _init_authentication(self):
        if self._conf.solr_username is not None and self._conf.solr_password is not None:
            return HTTPBasicAuth(self._conf.solr_username,
                                 self._conf.solr_password)
        return None

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
            msg = "Could not read file %s: %s" % (self._xml_file, str(e))
            logger.error(msg)
            return False, msg

        mydoc.check_mmd()  # Determine if we need to run this check.

        """ Convert mmd file to solr format """
        try:
            newdoc = mydoc.tosolr()
        except Exception as e:
            msg = 'Could not process the file %s: %s' % (
                self._xml_file, str(e))
            logger.error(msg)
            return False, msg

        """Check if document already exsists. Then we throw error and don't index."""
        isIndexed = self.mysolr.get_dataset(newdoc['id'])
        if isIndexed['doc'] is not None and self._cmd == DistCmd.INSERT:
            msg = "Document already exists in index, %s" % newdoc['metadata_identifier']
            logger.error(msg)
            return False, msg

        if 'related_dataset' in newdoc:
            logger.debug("Child dataset with id: %s",
                         newdoc['metadata_identifier'])
            logger.debug("Child dataset's parent's id is: %s",
                         newdoc['related_dataset'])
            parentid = newdoc['related_dataset_id']
            status, msg = self.mysolr.update_parent(
                parentid,
                fail_on_missing=self._conf.fail_on_missing_parent
            )
            if status:
                status, msg = self._index_record(
                    newdoc, add_thumbnail=False, level=2)
            else:
                return status, msg
        else:
            logger.debug("Parent/Level-1 - dataset - %s",
                         newdoc['metadata_identifier'])
            status, msg = self._index_record(
                newdoc, add_thumbnail=False, level=1)

        return status, msg

    def _index_record(self, newdoc, add_thumbnail=False, level=1):
        """ Wrapper function to return correct parameters (status and msg).
        """
        try:
            status, msg = self.mysolr.index_record(
                newdoc, addThumbnail=add_thumbnail, level=level)
            logger.info("Indexed document %s in SolR"
                        % newdoc['metadata_identifier'])
        except Exception as e:
            msg = "Could not index file %s, in SolR. Reason: %s" % (
                self._xml_file, str(e))
            logger.error(msg)
            status = False
        return status, msg

    def _delete(self):
        """Delete entry with a specified metadata_id."""
        identifier = self._construct_identifier(self._worker._namespace,
                                                self._metadata_id)
        status, msg = self.mysolr.delete(
            identifier, commit=self._conf.commit_on_delete)
        logger.info("SolR delete status: %s. With response: %s" %
                    (str(status), str(msg)))
        # return status, message
        return status, msg

# END Class SolRDist

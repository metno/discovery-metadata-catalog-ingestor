"""
DMCI : Distributor Super Class
==============================

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

import os
import uuid
import logging

from enum import Enum

from dmci import CONFIG

logger = logging.getLogger(__name__)


class DistCmd(Enum):

    INSERT = 0
    UPDATE = 1
    DELETE = 2

# END Enum DistCmd


class Distributor():

    def __init__(self, cmd, xml_file=None, metadata_UUID=None, worker=None, **kwargs):

        self._conf = CONFIG
        self._valid = False

        self._cmd = None
        self._xml_file = None
        self._path_to_parent_list = None
        self._metadata_UUID = None
        self._worker = worker
        self._kwargs = kwargs

        tmpcmd = str(cmd).upper()
        if tmpcmd in DistCmd.__members__:
            self._cmd = DistCmd[tmpcmd]
            self._valid = True
        else:
            logger.error("Unsupported command '%s'" % str(cmd))
            self._valid = False
            return

        if (xml_file is None) ^ (metadata_UUID is None):
            if xml_file is not None:
                if os.path.isfile(xml_file):
                    self._xml_file = xml_file
                    self._valid = True
                else:
                    logger.error("File does not exist: %s" % str(xml_file))
                    self._valid = False
                    return

            if metadata_UUID is not None:
                if isinstance(metadata_UUID, uuid.UUID) and metadata_UUID:
                    self._metadata_UUID = metadata_UUID
                    self._valid = True
                else:
                    logger.error("Metadata identifier must be a valid UUID")
                    self._valid = False
                    return
        else:
            logger.error("Either xml_file or metadata_UUID must be specified, but not both")
            self._valid = False
            return

        self._path_to_parent_list = kwargs.get('path_to_parent_list', None)
        if self._path_to_parent_list is not None:
            if os.path.isfile(kwargs['path_to_parent_list']):
                self._path_to_parent_list = kwargs['path_to_parent_list']
                self._valid = True
            else:
                logger.error("File does not exist: %s" % str(kwargs['path_to_parent_list']))
                self._valid = False
                return

        # Check consistency between command and data
        if self._cmd in (DistCmd.UPDATE, DistCmd.INSERT) and self._xml_file is None:
            logger.error("Command '%s' requires `xml_file` to be specified" % str(cmd))
            self._valid = False
            return

        if self._cmd == DistCmd.DELETE and self._metadata_UUID is None:
            logger.error("Command '%s' requires `metadata_UUID` to be specified" % str(cmd))
            self._valid = False
            return

        return

    def run(self):
        """The main run function to be implemented in each subclass."""
        raise NotImplementedError

    ##
    #  Getters
    ##

    def is_valid(self):
        return self._valid

    # Should be possible to remove this is we consistently use uri:UUID
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
# END Class Distributor

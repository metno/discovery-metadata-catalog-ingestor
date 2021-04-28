# -*- coding: utf-8 -*-
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
import logging

from enum import Enum

logger = logging.getLogger(__name__)


class DistCmd(Enum):

    INSERT = 0
    UPDATE = 1
    DELETE = 2


class Distributor():

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):

        self._valid = False

        self._cmd = None
        self._xml_file = None
        self._metadata_id = None
        self._kwargs = kwargs

        tmpcmd = str(cmd).upper()
        if tmpcmd in DistCmd.__members__:
            self._cmd = DistCmd[tmpcmd]
            self._valid = True
        else:
            logger.error("Unsupported command '%s'" % str(cmd))

        if (xml_file is None) ^ (metadata_id is None):
            if xml_file is not None:
                if os.path.isfile(xml_file):
                    self._xml_file = xml_file
                    self._valid = True
                else:
                    logger.error("File does not exist: %s" % str(xml_file))

            if metadata_id is not None:
                if isinstance(metadata_id, str) and metadata_id:
                    self._metadata_id = metadata_id
                    self._valid = True
                else:
                    logger.error("Metadata identifier must be a non-empty string")

        else:
            logger.error("Either xml_file or metadata_id must be specified, but not both")

        return

    def run(self):
        """The main run function to be implemented in each subclass.
        """
        if not self._valid:
            return False

        return False

# END Class Distributor

# -*- coding: utf-8 -*-
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

from dmci import CONFIG
from dmci.distributors import GitDist

logger = logging.getLogger(__name__)

class Worker():

    CALL_MAP = {
        "git": GitDist,
        # "pycsw": PyCSWDist,
    }

    def __init__(self, conn, **kwargs):

        # These dhould be populated with proper values to send to the
        # distributors

        self._dist_cmd = "insert"
        self._dist_xml_file = "test.xml"
        self._dist_metadata_id = None
        self._kwargs = {}

        return

    def validate(self):
        """Dummy function for the validator code.
        """
        code = 200
        msg = ""
        return code, msg

    def distribute(self):
        """Loop through all distributors listed in the config and call
        them in the same order.
        """
        status = True
        for dist in CONFIG.call_distributors:
            if dist in self.CALL_MAP:
                obj = self.CALL_MAP[dist](
                    self._dist_cmd,
                    xml_file=self._dist_xml_file,
                    metadata_id=self._dist_metadata_id,
                    **self._kwargs
                )
                status &= obj.run()

        return status

# END Class Worker

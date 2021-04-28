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
import os
import uuid

from dmci import CONFIG
from dmci.distributors import GitDist

logger = logging.getLogger(__name__)


class Worker():
    CALL_MAP = {
        "git": GitDist,
        # "pycsw": PyCSWDist,
    }

    def __init__(self,_CONFIG=None, **kwargs):
        if _CONFIG:
            self._conf = _CONFIG
        else:
            self._conf = CONFIG

        # These should be populated with proper values to send to the
        # distributors

        self._dist_cmd = "insert"
        self._dist_xml_file = "test.xml"
        self._dist_metadata_id = None
        self._kwargs = kwargs

        return

    def validate(self,data):
        """Dummy function for the validator code.
        """
        # Takes in bytes-object data
        # Gives msg when both validating and not validating
        if data == bytes("<xml: notXml", "utf-8"):
            return False, "Fails"
        return True, ""
        

    def distribute(self):
        """Loop through all distributors listed in the config and call
        them in the same order.
        """
        status = True
        valid  = True
        called = []
        failed = []
        skipped = []

        for dist in self._conf.call_distributors:
            if dist not in self.CALL_MAP:
                skipped.append(dist)
                continue

            obj = self.CALL_MAP[dist](
                self._dist_cmd,
                xml_file=self._dist_xml_file,
                metadata_id=self._dist_metadata_id,
                **self._kwargs
            )
            valid &= obj.is_valid()
            if obj.is_valid:
                obj_status = obj.run()
                status &= obj_status
                if obj_status:
                    called.append(dist)
                else:
                    failed.append(dist)

        return status, valid, called, failed, skipped

    def pushToQueues(self, data):
        file_uuid = uuid.uuid4()
        for path in self._conf.distributorQueuePaths:
            print(path)
            full_path = os.path.join(path, "{}.Q".format(file_uuid))
            with open(full_path, "wb") as queuefile:
                queuefile.write(data)


# END Class Worker

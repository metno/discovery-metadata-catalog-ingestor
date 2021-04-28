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

distributorPathList = CONFIG.distributorQueuePaths


# FIX ME: needs to be imported from validator
def validate_mmd(data):
    # Takes in bytes-object data
    # Gives msg when both validating and not validating
    if data == bytes("<xml: notXml", "utf-8"):
        return False, "Fails"
    return True, "Checks out"


# FIX ME: needs to be abstracted to config

logger = logging.getLogger(__name__)


class Worker():
    def __init__(self, conn):
        return

    def validate(self):
        return

    def distribute(self):
        return

    def pushToQueues(self, data):
        file_uuid = uuid.uuid4()
        for path in CONFIG.distributorQueuePaths:
            full_path = os.path.join(path, "{}.Q".format(file_uuid))
            with open(full_path, "wb") as queuefile:
                queuefile.write(data)


# END Class Worker

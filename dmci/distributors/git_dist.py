# -*- coding: utf-8 -*-
"""
DMCI : Git Distributor
======================

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

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)

class GitDist(Distributor):

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, **kwargs)

        return

    def run(self):
        Distributor.run(self)

        if self._cmd == DistCmd.UPDATE:
            pass
        elif self._cmd == DistCmd.INSERT:
            pass
        elif self._cmd == DistCmd.DELETE:
            pass
        else:
            logger.error("Invalid command: %s" % str(self._cmd))
            return False

        return True

# END Class GitDist

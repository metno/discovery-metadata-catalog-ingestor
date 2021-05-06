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

import os
import shutil
import logging
import datetime

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)

class GitDist(Distributor):

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, **kwargs)

        return

    def run(self):
        """Wrapper for the various jobs, depending on command.
        """
        if not self.is_valid():
            return False

        if self._cmd == DistCmd.UPDATE:
            return self._append_job()
        elif self._cmd == DistCmd.INSERT:
            return self._append_job()
        elif self._cmd == DistCmd.DELETE:
            logger.error("The `delete' command is not implemented")
            return False

        logger.error("Invalid command: %s" % str(self._cmd))

        return False

    def _append_job(self):
        """Append the xml file to the job queue.
        """
        jobsDir = self._conf.git_jobs_path
        if jobsDir is None:
            logger.error("No 'git_jobs_path' set")
            return False

        jobName = None
        jobPath = None
        for i in range(1000000):
            jobName = "%s_N%06d.xml" % (datetime.datetime.now().strftime("%Y%m%d_%H%M%S"), i)
            jobPath = os.path.join(jobsDir, jobName)
            if not os.path.isfile(jobPath):
                break

        if jobName is None or jobPath is None:
            logger.error("Failed to generate a unique job name")
            return False

        try:
            shutil.copy2(self._xml_file, jobPath)
        except Exception as e:
            logger.error("Failed to create job file")
            logger.error(str(e))
            return False

        return True

# END Class GitDist

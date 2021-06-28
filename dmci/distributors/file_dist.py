"""
DMCI : File Distributor
=======================

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
import shutil
import logging

from dmci.distributors.distributor import Distributor, DistCmd

logger = logging.getLogger(__name__)


class FileDist(Distributor):

    def __init__(self, cmd, xml_file=None, metadata_id=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, **kwargs)

        return

    def run(self):
        """Wrapper for the various jobs, depending on command."""
        if not self.is_valid():
            return False

        if self._cmd == DistCmd.INSERT:
            return self._add_to_archive()
        elif self._cmd == DistCmd.UPDATE:
            return self._add_to_archive()
        elif self._cmd == DistCmd.DELETE:
            logger.error("The `delete' command is not implemented")
            return False

        logger.error("Invalid command: %s" % str(self._cmd))

        return False

    ##
    #  Internal Functions
    ##

    def _add_to_archive(self):
        """Add the xml file to the archive."""
        jobsDir = self._conf.file_archive_path
        if jobsDir is None:
            logger.error("No 'file_archive_path' set")
            return False

        if self._worker is None:
            logger.error("No worker object sent to file_dist")
            return False

        fileUUID = self._worker._file_metadata_id
        if not isinstance(fileUUID, uuid.UUID):
            logger.error("No valid metadata_identifier provided, cannot archive file")
            return False

        lvlA = "arch_%s" % fileUUID.hex[7]
        lvlB = "arch_%s" % fileUUID.hex[6]
        lvlC = "arch_%s" % fileUUID.hex[5]

        archPath = os.path.join(self._conf.file_archive_path, lvlA, lvlB, lvlC)
        archFile = os.path.join(archPath, str(fileUUID)+".xml")

        status = "added"
        if os.path.isfile(archFile):
            if self._cmd == DistCmd.UPDATE:
                status = "replaced"
            else:
                logger.error("File already exists: %s" % archFile)
                return False

        try:
            os.makedirs(archPath, exist_ok=True)
            logger.info("Created folder: %s" % archPath)
        except Exception as e:
            logger.error("Could not make folder(s): %s" % archPath)
            logger.error(str(e))
            return False

        try:
            shutil.copy2(self._xml_file, archFile)
        except Exception as e:
            logger.error("Failed to archive file src: %s" % self._xml_file)
            logger.error("Failed to archive file dst: %s" % archFile)
            logger.error(str(e))
            return False

        logger.info("%s file: %s" % (status.title(), archFile))

        return True

# END Class FileDist

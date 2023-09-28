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


def get_folder_names(fileUUID):
    lvlA = "arch_%s" % fileUUID.hex[7]
    lvlB = "arch_%s" % fileUUID.hex[6]
    lvlC = "arch_%s" % fileUUID.hex[5]

    return lvlA, lvlB, lvlC


class FileDist(Distributor):

    def __init__(self, cmd, xml_file=None, metadata_UUID=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_UUID, **kwargs)

        return

    def run(self):
        """Wrapper for the various jobs, depending on command."""
        status = False
        msg = "No job was run"
        if not self.is_valid():
            return False, "The run job is invalid"

        if self._cmd == DistCmd.INSERT:
            status, msg = self._add_to_archive()
        elif self._cmd == DistCmd.UPDATE:
            status, msg = self._add_to_archive()
        elif self._cmd == DistCmd.DELETE:
            status, msg = self._delete_from_archive()

        return status, msg

    ##
    #  Internal Functions
    ##

    def _add_to_archive(self):
        """Add the xml file to the archive."""
        jobsDir = self._conf.file_archive_path
        if jobsDir is None:
            logger.error("No 'file_archive_path' set")
            return False, "Internal error"

        if self._worker is None:
            logger.error("No worker object sent to file_dist")
            return False, "Internal error"

        fileUUID = self._worker._file_metadata_id
        if not isinstance(fileUUID, uuid.UUID):
            msg = "No valid metadata_identifier provided, cannot archive file"
            logger.error(msg)
            return False, msg

        fileName, archPath = self._make_full_path(fileUUID)
        archFile = os.path.join(archPath, fileName)

        if os.path.isfile(archFile):
            if self._cmd == DistCmd.UPDATE:
                status = "replaced"
            else:  # INSERT
                logger.error("File already exists: %s", archFile)
                return False, "File already exists: %s" % fileName
        else:
            if self._cmd == DistCmd.INSERT:
                status = "added"
            else:  # UPDATE
                logger.error("Cannot update non-existing file: %s", archFile)
                return False, "Cannot update non-existing file: %s" % fileName

        try:
            os.makedirs(archPath, exist_ok=True)
            logger.info("Created folder: %s", archPath)
        except Exception as e:
            logger.error("Could not make folder(s): %s", archPath)
            logger.error(str(e))
            return False, "Failed to archive file: %s" % fileName

        try:
            shutil.copy2(self._xml_file, archFile)
        except Exception as e:
            logger.error("Failed to archive file src: %s", self._xml_file)
            logger.error("Failed to archive file dst: %s", archFile)
            logger.error(str(e))
            return False, "Failed to archive file: %s" % fileName

        msg = "%s file: %s" % (status.title(), fileName)
        logger.info(msg)

        return True, msg

    def _delete_from_archive(self):
        """Delete a file from the archive."""
        fileUUID = self._metadata_UUID
        if not isinstance(fileUUID, uuid.UUID):
            msg = "No valid metadata_identifier provided, cannot delete file"
            logger.error(msg)
            return False, msg

        fileName, archPath = self._make_full_path(fileUUID)
        archFile = os.path.join(archPath, fileName)

        if os.path.isfile(archFile):
            try:
                os.unlink(archFile)
            except Exception as e:
                logger.error("Failed to delete file: %s", archFile)
                logger.error(str(e))
                return False, "Failed to delete file: %s" % fileName
        else:
            logger.error("File not found: %s", archFile)
            return False, "File not found: %s" % fileName

        return True, "Deleted file: %s" % fileName

    def _make_full_path(self, fileUUID):
        """Make the file name and path for a file with a given uuid."""
        lvlA, lvlB, lvlC = get_folder_names(fileUUID)

        fileName = str(fileUUID) + ".xml"
        archPath = os.path.join(self._conf.file_archive_path, lvlA, lvlB, lvlC)

        return fileName, archPath

# END Class FileDist

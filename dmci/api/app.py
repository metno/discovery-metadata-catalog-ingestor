"""
DMCI : API App Class
====================

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
import sys
import uuid
import shutil

from flask import Flask, request
from lxml import etree

import dmci
from dmci.api.worker import Worker

logger = logging.getLogger(__name__)

OK_RETURN = "Everything is OK"


class App(Flask):

    def __init__(self):
        super().__init__(__name__)
        self._conf = dmci.CONFIG

        if self._conf.distributor_cache is None:
            logger.error("Parameter distributor_cache in config is not set")
            sys.exit(1)

        if self._conf.mmd_xsd_path is None:
            logger.error("Parameter mmd_xsd_path in config is not set")
            sys.exit(1)

        if self._conf.path_to_parent_list is None:
            logger.error("Parameter path_to_parent_list in config is not set")
            sys.exit(1)

        # Create the XML Validator Object
        try:
            self._xsd_obj = etree.XMLSchema(
                etree.parse(self._conf.mmd_xsd_path))
        except Exception as e:
            logger.critical("XML Schema could not be parsed: %s" %
                            str(self._conf.mmd_xsd_path))
            logger.critical(str(e))
            sys.exit(1)

        # Set up api entry points
        @self.route("/v1/create", methods=["POST"])
        @self.route("/v1/insert", methods=["POST"])
        def post_insert():
            msg, code = self._insert_update_method_post("insert", request)
            return self._formatMsgReturn(msg), code

        @self.route("/v1/update", methods=["POST"])
        def post_update():
            msg, code = self._insert_update_method_post("update", request)
            return self._formatMsgReturn(msg), code

        @self.route("/v1/delete/<metadata_id>", methods=["POST"])
        def post_delete(metadata_id=None):
            """Process delete command."""
            md_namespace, md_uuid, err = self._check_namespace_UUID(
                metadata_id)

            if md_uuid is not None:
                worker = Worker("delete", None, self._xsd_obj,
                                md_uuid=md_uuid, md_namespace=md_namespace)
                err = self._distributor_wrapper(worker)
            else:
                return self._formatMsgReturn(err), 400

            if err:
                return self._formatMsgReturn(err), 500
            else:
                return self._formatMsgReturn(OK_RETURN), 200

        @self.route("/v1/validate", methods=["POST"])
        def post_validate():
            """Process validate command."""
            msg, code = self._validate_method_post(request)
            return self._formatMsgReturn(msg), code

        return

    ##
    #  Internal Functions
    ##

    def _formatMsgReturn(self, msg):
        """Formats the return message depending on its type and ensures
        that it has proper line breaks for usage with curl.
        """
        fmtMsg = ""
        if isinstance(msg, list):
            fmtMsg = "\n".join(msg)
        else:
            fmtMsg = str(msg)
        return fmtMsg.rstrip() + "\n"

    def _insert_update_method_post(self, cmd, request):
        """Process insert or update command requests."""
        if request.content_length > self._conf.max_permitted_size:
            return f"The file is larger than maximum size: {self._conf.max_permitted_size}", 413

        data = request.get_data()

        # Cache the job file
        file_uuid = uuid.uuid4()
        full_path = os.path.join(
            self._conf.distributor_cache, f"{file_uuid}.xml")
        reject_path = os.path.join(
            self._conf.rejected_jobs_path, f"{file_uuid}.xml")
        msg, code = self._persist_file(data, full_path)
        if code != 200:
            return msg, code

        # Run the validator
        worker = Worker(cmd, full_path, self._xsd_obj,
                        path_to_parent_list=self._conf.path_to_parent_list)
        valid, msg, data_ = worker.validate(data)
        if not valid:
            msg += f"\n UUID : {file_uuid} \n "
            self._handle_persist_file(False, full_path, reject_path, msg)
            return msg, 400

        # Check if the data from the request was modified in worker.validate().
        # If so we will need to write the modified data to disk.
        if not data == data_:
            msg, code = self._persist_file(data_, full_path)
            if code != 200:
                return msg, code

        # Run the distributors
        err = self._distributor_wrapper(worker)

        if err:
            msg = "\n".join(err)
            self._handle_persist_file(False, full_path, reject_path, msg)
            return msg, 500
        else:
            self._handle_persist_file(True, full_path)
            return OK_RETURN, 200

    def _validate_method_post(self, request):
        """Only run the validator for submitted file."""
        if request.content_length > self._conf.max_permitted_size:
            return f"The file is larger than maximum size: {self._conf.max_permitted_size}", 413

        data = request.get_data()

        # Cache the job file
        file_uuid = uuid.uuid4()
        full_path = os.path.join(
            self._conf.distributor_cache, f"{file_uuid}.xml")
        msg, code = self._persist_file(data, full_path)
        if code != 200:
            self._handle_persist_file(True, full_path)
            return msg, code

        # Run the validator
        worker = Worker("none", full_path, self._xsd_obj,
                        path_to_parent_list=self._conf.path_to_parent_list)
        valid, msg, data = worker.validate(data)
        self._handle_persist_file(True, full_path)
        if valid:
            return OK_RETURN, 200
        else:
            return msg, 400

    def _distributor_wrapper(self, worker):
        """Run the distributors and handle and parse the results and
        parse and combine any error messages.
        """
        err = []
        status, valid, _, failed, skipped, failed_msg = worker.distribute()
        if not status:
            err.append("The following distributors failed: %s" %
                       ", ".join(failed))
            for name, reason in zip(failed, failed_msg):
                err.append(" - %s: %s" % (name, reason))

        if not valid:
            err.append("The following jobs were skipped: %s" %
                       ", ".join(skipped))

        return err

    @staticmethod
    def _check_namespace_UUID(metadata_id):
        """Splits incoming metadata_id to namespace and UUID, assuming
        structure to be namespace:UUID
        """
        md_uuid, md_namespace, err = None, "", None
        elements = metadata_id.split(":")
        try:
            md_uuid = uuid.UUID(elements[-1])
            # Only UUID, no namespace
            if len(elements) < 2:
                md_namespace, err = "", None
            # namespace and UUID
            elif len(elements) == 2:
                md_namespace, err = elements[0], None
            else:
                err = "Malformed metadata id. Should be <namespace>:<UUID>."
                logger.error(err)
                return None, "", err

        except ValueError as e:
            err = f"Can not convert to UUID: {elements[-1]}"
            logger.error(err)
            logger.error(str(e))

        return md_namespace, md_uuid, err

    @staticmethod
    def _handle_persist_file(status, full_path, reject_path=None, reject_reason=""):
        """Handle the persistent file after it has been processed based
        on the status of the process. If successful, the file is just
        deleted. If not, the file is saved in a rejected folder for
        later manual inspection.
        """
        if status:
            try:
                os.unlink(full_path)
            except Exception as e:
                logger.error("Failed to unlink processed file: %s", full_path)
                logger.error(str(e))
                return False

        else:
            try:
                shutil.copy(full_path, reject_path)

            # If source and destination are same
            except shutil.SameFileError as e:
                logger.error(
                    "Source and destination represents the same file. %s -> %s"
                    % (full_path, reject_path))
                logger.error(str(e))
                return False

            # If there is any permission issue
            except PermissionError as e:
                logger.error("Permission denied")
                logger.error(str(e))
                return False

            # Handle other possible exceptions
            except shutil.Error as e:
                logger.error("Something failed moving the rejected file.")
                logger.error(str(e))
                return False

            else:
                os.remove(full_path)

            reason_path = reject_path[:-3] + "txt"
            try:
                with open(reason_path, mode="w", encoding="utf-8") as ofile:
                    ofile.write(reject_reason)
            except Exception as e:
                logger.error(
                    "Failed to write rejected reason to file: %s", reason_path)
                logger.error(str(e))
                return False

        return True

    @staticmethod
    def _persist_file(data, full_path):
        """Write the persistent file."""
        try:
            with open(full_path, "wb") as queuefile:
                queuefile.write(data)

        except Exception as e:
            logger.error(str(e))
            return "Cannot write xml data to cache file", 507

        return OK_RETURN, 200

# END Class App

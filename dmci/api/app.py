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

import dmci
import os
import sys
import uuid
import logging

from lxml import etree
from flask import request, Flask

from dmci.api.worker import Worker

logger = logging.getLogger(__name__)


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

        # Create the XML Validator Object
        try:
            self._xsd_obj = etree.XMLSchema(etree.parse(self._conf.mmd_xsd_path))
        except Exception as e:
            logger.critical("XML Schema could not be parsed: %s" % str(self._conf.mmd_xsd_path))
            logger.critical(str(e))
            sys.exit(1)

        # Set up api entry points
        @self.route("/v1/insert", methods=["POST"])
        def post_insert():
            return self._insert_update_method_post("insert", request)

        @self.route("/v1/update", methods=["POST"])
        def post_update():
            return self._insert_update_method_post("update", request)

        @self.route("/v1/delete/<uuid:metadata_id>", methods=["POST"])
        def post_delete(metadata_id=None):
            """Process delete command."""
            worker = Worker("delete", None, self._xsd_obj, metadata_id=metadata_id)
            err = self._distributor_wrapper(worker)
            if err:
                return "\n".join(err), 500
            else:
                return self._everything_is_ok()

        return

    ##
    #  Internal Functions
    ##

    def _everything_is_ok(self):
        return "Everything is OK\n", 200

    def _insert_update_method_post(self, cmd, request):
        """Process insert or update command requests."""
        if request.content_length > self._conf.max_permitted_size:
            return f"File bigger than permitted size: {self._conf.max_permitted_size}", 413

        data = request.get_data()

        # Cache the job file
        file_uuid = uuid.uuid4()
        full_path = os.path.join(self._conf.distributor_cache, f"{file_uuid}.xml")
        reject_path = os.path.join(self._conf.rejected_jobs_path, f"{file_uuid}.xml")
        msg, code = self._persist_file(data, full_path)
        if code != 200:
            return msg, code

        # Run the validator
        worker = Worker(cmd, full_path, self._xsd_obj)
        valid, msg = worker.validate(data)
        if not valid:
            self._handle_persist_file(False, full_path, reject_path, msg)
            return msg, 400

        # Run the distributors
        err = self._distributor_wrapper(worker)

        if err:
            msg = "\n".join(err)
            self._handle_persist_file(False, full_path, reject_path, msg)
            return msg, 500
        else:
            self._handle_persist_file(True, full_path)
            return self._everything_is_ok()

    def _distributor_wrapper(self, worker):
        """Run the distributors and handle and parse the results and
        parse and combine any error messages.
        """
        err = []
        status, valid, _, failed, skipped, failed_msg = worker.distribute()
        if not status:
            err.append("The following distributors failed: %s" % ", ".join(failed))
            for name, reason in zip(failed, failed_msg):
                err.append(" - %s: %s" % (name, reason))

        if not valid:
            err.append("The following jobs were skipped: %s" % ", ".join(skipped))

        return err

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
                os.rename(full_path, reject_path)
            except Exception as e:
                logger.error("Failed to move persist file to rejected folder: %s", reject_path)
                logger.error(str(e))
                return False

            try:
                reason_path = reject_path[:-3]+"txt"
                with open(reason_path, mode="w", encoding="utf-8") as ofile:
                    ofile.write(reject_reason)
            except Exception as e:
                logger.error("Failed to write rejected reason to file: %s", reason_path)
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

        return "", 200

# END Class App

# -*- coding: utf-8 -*-
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
import logging
import os
import sys
import uuid

from lxml import etree
from flask import request, Flask, after_this_request

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
        def base():
            max_permitted_size = self._conf.max_permitted_size

            if request.content_length > max_permitted_size:
                return f"File bigger than permitted size: {max_permitted_size}", 413

            data = request.get_data()

            file_uuid = uuid.uuid4()
            path = self._conf.distributor_cache
            full_path = os.path.join(path, f"{file_uuid}.Q")

            worker = Worker(full_path, self._xsd_obj)

            @after_this_request
            def dist(response):
                nonlocal worker
                worker.distribute()
                return response

            result, msg = worker.validate(data)
            if result:
                return self._persist_file(data, full_path)
            else:
                return msg, 500

            # TODO: shouldn't msg be logged?

        return

    ##
    #  Internal Functions
    ##

    @staticmethod
    def _persist_file(data, full_path):
        """Write the persistent file
        """
        try:
            with open(full_path, "wb") as queuefile:
                queuefile.write(data)

        except Exception as e:
            logger.error(str(e))
            return "Can't write to file", 507

        return "", 200

# END Class App

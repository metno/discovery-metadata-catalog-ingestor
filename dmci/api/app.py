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

import logging
import os
import uuid

from flask import request, Flask, after_this_request

from dmci import CONFIG
from dmci.api.worker import Worker

logger = logging.getLogger(__name__)

class App():

    def __init__(self):
        self._app = Flask(__name__)
        self._conf = CONFIG

        @self._app.route("/v1/insert", methods=["POST"])
        def base():
            data = request.get_data()

            max_permitted_size = self._conf.max_permitted_size

            if len(data) > max_permitted_size:
                return f"File bigger than permitted size: {max_permitted_size}", 413

            file_uuid = uuid.uuid4()
            path = self._conf.distributor_input_path
            full_path = os.path.join(path, f"{file_uuid}.Q")

            worker = Worker(full_path)

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

        return

    def _persist_file(self, data, full_path):
        try:
            with open(full_path, "wb") as queuefile:
                queuefile.write(data)

        except Exception as e:
            logger.error(str(e))
            return "Can't write to file", 507

        return "", 200

# END Class App
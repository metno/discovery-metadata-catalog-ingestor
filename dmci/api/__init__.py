"""
DMCI : Api init
=================

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

from flask import Flask, request
import logging
from dmci.worker import Worker
from dmci.api.api import Api


logger = logging.getLogger(__name__)


def create_app(_CONFIG = None):
    if _CONFIG:
        CONFIG = _CONFIG
    app = Api(__name__)
    app.set_worker(Worker(CONFIG))

    @app.route('/', methods=['POST'])
    def base():
        data = request.get_data()

        result, msg = app.worker.validate(data)

        if result:
            try:
                app.worker.pushToQueues(data)
            except Exception as e:
                logger.error(e)
                return "Can't save to disk", 507

            return msg, 200
        else:
            return msg, 500

    return app

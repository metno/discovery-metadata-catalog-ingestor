"""
DMCI : API Start Script
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
import sys

from prometheus_flask_exporter.multiprocess import GunicornInternalPrometheusMetrics
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app, CollectorRegistry
from prometheus_client.core import REGISTRY

from dmci.api import App
from dmci import CONFIG

if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
    sys.exit(1)


app = App()
REGISTRY.register(CollectorRegistry())
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app(REGISTRY)
})
GunicornInternalPrometheusMetrics(app,path='/metrics',registry=REGISTRY)

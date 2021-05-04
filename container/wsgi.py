# -*- coding: utf-8 -*-
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

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(
    1, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                    os.path.pardir)))

from dmci.api import App
from dmci import CONFIG

CONFIG.readConfig()

app = App()._app
# app._app.run()%     

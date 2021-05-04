# -*- coding: utf-8 -*-
"""
DMCI : External Tools Init
==========================

This package contains code extracted from py_mmd_tools that is needed
for this repository.

TODO: Integrate this code properly in this repository.

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

from dmci.external.xml_utils import xml_translate
from dmci.external.check_mmd import full_check

__all__ = [
    "xml_translate",
    "full_check",
]

# -*- coding: utf-8 -*-
"""
DMCI : Dummy Test
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

import dmci
import pytest

@pytest.mark.core
def test_dummyTest():
    """Just so there is something for pytest to do until we write any
    actual tests.
    """
    assert dmci.__version__ == "0.0.1"

# END Test test_dummyTest

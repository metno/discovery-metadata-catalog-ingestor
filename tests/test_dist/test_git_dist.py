# -*- coding: utf-8 -*-
"""
DMCI : Git Distributor Class Test
=================================

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
import pytest

from dmci.distributors import GitDist

@pytest.mark.dist
def testDistGit_Init():
    """Test the GitDist class init.
    """
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert GitDist("insert", metadata_id="some_id").is_valid() is False
    assert GitDist("update", metadata_id="some_id").is_valid() is False
    assert GitDist("delete", metadata_id="some_id").is_valid() is True
    assert GitDist("blabla", metadata_id="some_id").is_valid() is False

# END Test testDistGit_Init

@pytest.mark.dist
def testDistGit_Run(tmpDir):
    """Test the GitDist class run function.
    """
    assert GitDist("insert", metadata_id="some_id").run() is False
    assert GitDist("update", metadata_id="some_id").run() is False
    assert GitDist("delete", xml_file="/path/to/somewhere").run() is False
    assert GitDist("blabla", metadata_id="some_id").run() is False

# END Test testDistGit_Run

@pytest.mark.dist
def testDistGit_InsertUpdate(tmpDir, mockXml, monkeypatch):
    """Test the GitDist class insert and update actions.
    """
    gitDir = os.path.join(tmpDir, "git")
    jobsDir = os.path.join(gitDir, "jobs")

    os.mkdir(gitDir)
    os.mkdir(jobsDir)

    tstGit = GitDist("insert", xml_file=mockXml)
    tstGit._conf.git_jobs_path = jobsDir

    assert tstGit.run() is True

# END Test testDistGit_InsertUpdate

# -*- coding: utf-8 -*-
"""
DMCI : File Distributor Class Test
==================================

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
import time
import datetime

from tools import causeOSError

from dmci.distributors import FileDist
from dmci.distributors.distributor import DistCmd

@pytest.mark.dist
def testDistGit_Init():
    """Test the FileDist class init.
    """
    # Check that it initialises properly by running some of the simple
    # Distributor class tests
    assert FileDist("insert", metadata_id="some_id").is_valid() is False
    assert FileDist("update", metadata_id="some_id").is_valid() is False
    assert FileDist("delete", metadata_id="some_id").is_valid() is True
    assert FileDist("blabla", metadata_id="some_id").is_valid() is False

# END Test testDistGit_Init

@pytest.mark.dist
def testDistGit_Run(tmpDir, mockXml):
    """Test the FileDist class run function.
    """
    tstDist = FileDist("insert", xml_file=mockXml)
    assert tstDist.is_valid()

    tstDist._valid = False
    assert tstDist.run() is False
    tstDist._valid = True

    tstDist._append_job = lambda *a: True

    tstDist._cmd = DistCmd.INSERT
    assert tstDist.run() is True

    tstDist._cmd = DistCmd.UPDATE
    assert tstDist.run() is True

    tstDist._cmd = DistCmd.DELETE
    assert tstDist.run() is False

    tstDist._cmd = 1234
    assert tstDist.run() is False

# END Test testDistGit_Run

@pytest.mark.dist
def testDistGit_InsertUpdate(tmpDir, mockXml, monkeypatch):
    """Test the FileDist class insert and update actions.
    """
    monkeypatch.setattr(time, "time", lambda: 123.0)
    fileDir = os.path.join(tmpDir, "file")
    jobsDir = os.path.join(fileDir, "jobs")

    jobTime = datetime.datetime.fromtimestamp(123.0).strftime("%Y%m%d_%H%M%S")
    jobProc = os.getpid()

    os.mkdir(fileDir)
    os.mkdir(jobsDir)

    tstGit = FileDist("insert", xml_file=mockXml)
    assert tstGit._append_job() is False

    # Generate a job file
    tstGit._conf.file_jobs_path = jobsDir
    assert tstGit.run() is True
    jobName = "%s_N%05d_P%d.xml" % (jobTime, 0, jobProc)
    assert os.path.isfile(os.path.join(jobsDir, jobName))

    # Generate a job second file
    tstGit._conf.file_jobs_path = jobsDir
    assert tstGit.run() is True
    jobName = "%s_N%05d_P%d.xml" % (jobTime, 1, jobProc)
    assert os.path.isfile(os.path.join(jobsDir, jobName))

    # Should fail because the job path is None
    with monkeypatch.context() as mp:
        mp.setattr("os.path.join", lambda *a: None)
        mp.setattr("os.path.isfile", lambda *a: False)
        assert tstGit.run() is False

    # Should fail because it cannot generate a unique file name
    with monkeypatch.context() as mp:
        mp.setattr("os.path.isfile", lambda *a: True)
        assert tstGit.run() is False

    # Should fail because the copy command fails
    with monkeypatch.context() as mp:
        mp.setattr("shutil.copy2", causeOSError)
        assert tstGit.run() is False

# END Test testDistGit_InsertUpdate

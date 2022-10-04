"""
DMCI : Distributor Class Test
=============================

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

import pytest

from dmci.distributors.distributor import Distributor


@pytest.mark.dist
def testDistDistributor_Init(mockXml, tmpUUID):
    """Test the Distributor super class init."""

    # Check Insert Command
    assert Distributor("insert", metadata_id=tmpUUID).is_valid() is False
    assert Distributor("insert", xml_file="/path/to/nowhere").is_valid() is False
    assert Distributor("insert", xml_file=mockXml).is_valid() is True
    assert Distributor("inSeRt", xml_file=mockXml).is_valid() is True
    assert Distributor("insert", xml_file=mockXml, metadata_id="stuff").is_valid() is False
    assert Distributor("insert", xml_file=mockXml,
                       path_to_parent_list="/path/to/nowhere").is_valid() is False

    # Check Update Command
    assert Distributor("update", metadata_id=tmpUUID).is_valid() is False
    assert Distributor("update", xml_file="/path/to/nowhere").is_valid() is False
    assert Distributor("update", xml_file=mockXml).is_valid() is True
    assert Distributor("uPdatE", xml_file=mockXml).is_valid() is True
    assert Distributor("update", xml_file=mockXml, metadata_id="stuff").is_valid() is False

    # Check Delete Command
    assert Distributor("delete", xml_file=mockXml).is_valid() is False
    assert Distributor("delete", metadata_id=None).is_valid() is False
    assert Distributor("delete", metadata_id=1234).is_valid() is False
    assert Distributor("delete", metadata_id=tmpUUID).is_valid() is True
    assert Distributor("deLEte", metadata_id=tmpUUID).is_valid() is True
    assert Distributor("delete", xml_file=mockXml, metadata_id=tmpUUID).is_valid() is False

    # Check Unsupported Command
    assert Distributor("blabla", metadata_id=tmpUUID).is_valid() is False
    assert Distributor("blabla", xml_file=mockXml).is_valid() is False
    assert Distributor(12345678, metadata_id=tmpUUID).is_valid() is False
    assert Distributor(12345678, xml_file=mockXml).is_valid() is False

# END Test testDistDistributor_Init


@pytest.mark.dist
def testDistDistributor_Run(tmpUUID):
    """Test the Distributor super class run function.

    Calling run() on the superclass should raise an error as this
    function must be implemented in subclasses.
    """
    with pytest.raises(NotImplementedError):
        Distributor("insert", metadata_id=tmpUUID).run()

# END Test testDistDistributor_Run

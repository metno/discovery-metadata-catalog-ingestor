# -*- coding: utf-8 -*-
"""
DMCI : Test Tools
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

# File I/O

def readFile(fileName):
    """Returns the content of a file as a string.
    """
    with open(fileName, mode="r", encoding="utf8") as inFile:
        return inFile.read()

def writeFile(fileName, fileData):
    """Write the contents of a string to a file.
    """
    with open(fileName, mode="w", encoding="utf8") as outFile:
        outFile.write(fileData)

# Exceptions

def causeOSError(*args, **kwargs):
    """This function will take any arguments and always raise an
    OSError when called.
    """
    raise OSError("Test OSError")

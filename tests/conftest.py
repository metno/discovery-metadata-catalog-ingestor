# -*- coding: utf-8 -*-
"""
DMCI : Test Config
==================

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
import shutil
import pytest

from tools import writeFile

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

@pytest.fixture(scope="session")
def tmpDir():
    """A temporary folder for the test session. This folder is
    presistent after the tests have run so that the status of generated
    files can be checked. The folder is instead cleared before a new
    test session.
    """
    testDir = os.path.dirname(__file__)
    theDir = os.path.join(testDir, "temp")
    if os.path.isdir(theDir):
        shutil.rmtree(theDir)
    if not os.path.isdir(theDir):
        os.mkdir(theDir)
    return theDir

@pytest.fixture(scope="session")
def refDir():
    """A path to the reference files folder.
    """
    testDir = os.path.dirname(__file__)
    theDir = os.path.join(testDir, "files")
    return theDir

@pytest.fixture(scope='session')
def dummy_xml_str():
    return "<xml />"

@pytest.fixture(scope='session')
def distDir(tmpDir):
    distDir = os.path.join(tmpDir, "dist")
    if not os.path.isdir(distDir):
        os.mkdir(distDir)
    return distDir

@pytest.fixture(scope='session')
def dummyXml(distDir, dummy_xml_str):
    dummyXml = os.path.join(distDir, "dummy.xml")
    writeFile(dummyXml, dummy_xml_str)
    return dummyXml

@pytest.fixture(scope='session')
def dummyXslt(distDir):
    dummyXslt = os.path.join(distDir, "dummy.xslt")
    writeFile(dummyXslt, "<xml />")
    return dummyXslt

@pytest.fixture(scope='session')
def transactionResponseInsert():
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
                    'xmlns:dc="http://purl.org/dc/elements/1.1/" '
                    'xmlns:dct="http://purl.org/dc/terms/" '
                    'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
                    'xmlns:gml="http://www.opengis.net/gml" '
                    'xmlns:ows="http://www.opengis.net/ows" '
                    'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
                    'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
                    'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
                '<csw:TransactionSummary>'
                    '<csw:totalInserted>1</csw:totalInserted>'
                    '<csw:totalUpdated>0</csw:totalUpdated>'
                    '<csw:totalDeleted>0</csw:totalDeleted>'
                '</csw:TransactionSummary>'
                '<csw:InsertResult>'
                    '<csw:BriefRecord>'
                    '<dc:identifier>'
                        'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
                    '</dc:identifier>'
                    '<dc:title>Date: 2020-04-20T02:32:44.006Z, Instrument: SAR-C SAR, '
                        'Mode: HH HV, Satellite: Sentinel-1, Size: 434.58 MB'
                    '</dc:title>'
                    '</csw:BriefRecord>'
                '</csw:InsertResult>'
            '</csw:TransactionResponse>')
    return text

@pytest.fixture(scope='session')
def exceptionReportInsertAlreadyExists():
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<ows:ExceptionReport xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dct="http://purl.org/dc/terms/" '
            'xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" '
            'xmlns:ows="http://www.opengis.net/ows" xmlns:xs="http://www.w3.org/2001/XMLSchema" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.2.0" '
            'language="en-US" xsi:schemaLocation="http://www.opengis.net/ows '
            'http://schemas.opengis.net/ows/1.0.0/owsExceptionReport.xsd">'
            '<ows:Exception exceptionCode="NoApplicableCode" locator="insert">'
            '<ows:ExceptionText>Transaction (insert) failed: (psycopg2.errors.UniqueViolation) '
            'duplicate key value violates unique constraint "records_pkey"'
            'DETAIL: '
            'Key (identifier)=('
            'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB) '
            'already exists.'
            '</ows:ExceptionText></ows:Exception></ows:ExceptionReport>')
    return text

@pytest.fixture(scope='session')
def transactionResponseDelete():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
                'xmlns:dc="http://purl.org/dc/elements/1.1/" '
                'xmlns:dct="http://purl.org/dc/terms/" '
                'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
                'xmlns:gml="http://www.opengis.net/gml" xmlns:ows="http://www.opengis.net/ows" '
                'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
                'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
                'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
                '<csw:TransactionSummary>'
                    '<csw:totalInserted>0</csw:totalInserted>'
                    '<csw:totalUpdated>0</csw:totalUpdated>'
                    '<csw:totalDeleted>1</csw:totalDeleted>'
                '</csw:TransactionSummary>'
            '</csw:TransactionResponse>')
    return [id, text]

@pytest.fixture(scope='session')
def transactionResponseDeleteFails():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<csw:TransactionResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" '
                'xmlns:dc="http://purl.org/dc/elements/1.1/" '
                'xmlns:dct="http://purl.org/dc/terms/" '
                'xmlns:gmd="http://www.isotc211.org/2005/gmd" '
                'xmlns:gml="http://www.opengis.net/gml" xmlns:ows="http://www.opengis.net/ows" '
                'xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.2" '
                'xsi:schemaLocation="http://www.opengis.net/cat/csw/2.0.2 '
                'http://schemas.opengis.net/csw/2.0.2/CSW-publication.xsd">'
                '<csw:TransactionSummary>'
                    '<csw:totalInserted>0</csw:totalInserted>'
                    '<csw:totalUpdated>0</csw:totalUpdated>'
                    '<csw:totalDeleted>0</csw:totalDeleted>'
                '</csw:TransactionSummary>'
            '</csw:TransactionResponse>')
    return [id, text]

@pytest.fixture(scope='session')
def transactionResponseDeleteUnknownTagName():
    id = 'S1A_EW_GRDM_1SDH_20200420T023244_20200420T023348_032205_03B97F_72EB'
    text = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            '<!-- pycsw 2.7.dev0 -->'
            '<SomeNewTagName>'
                '<TransactionSummary>'
                    '<totalInserted>0</totalInserted>'
                    '<totalUpdated>0</totalUpdated>'
                    '<totalDeleted>1</totalDeleted>'
                '</TransactionSummary>'
            '</SomeNewTagName>')
    return [id, text]

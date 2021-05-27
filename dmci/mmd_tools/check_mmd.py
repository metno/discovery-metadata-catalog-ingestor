# -*- coding: utf-8 -*-
"""
DMCI : MMD Checker Functions
============================

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

import logging
import pythesint as pti

from lxml import etree
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def check_rectangle(rectangle):
    """Check if element geographic extent/rectangle is valid:
        - only 1 existing rectangle element
        - rectangle has north / south / west / east subelements
        - -180 <= min_lat <= max_lat <= 180
        -    0 <= min_lon <= max_lon <= 360
    Args:
        rectangle: list of elements found when requesting node(s) geographic_extent/rectangle
          (output of ET request findall)
    Returns:
        True / False
    """
    directions = dict.fromkeys(['north', 'south', 'west', 'east'], None)

    ok = True
    if len(rectangle) > 1:
        logger.debug("NOK: Multiple rectangle elements in file.")
        return False

    for child in rectangle[0]:
        # Also removes namespace, if any
        child_ns = etree.QName(child)
        directions[child_ns.localname] = float(child.text)

    for key, val in directions.items():
        if val is None:
            logger.error("NOK: Missing rectangle element %s" % key)
            return False

    if not (-180 <= directions['west'] <= directions['east'] <= 180):
        logger.debug("NOK: Longitudes not ok")
        ok = False
    if not (-90 <= directions['south'] <= directions['north'] <= 90):
        logger.debug("NOK: Latitudes not ok")
        ok = False
    if not ok:
        logger.debug(directions)

    return ok

def check_url(url, allow_no_path=False):
    """Check that an URL is valid.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https", "ftp", "sftp"):
            logger.debug(f"NOK: {url}")
            logger.debug("URL scheme not allowed")
            return False

        if not (parsed.netloc and "." in parsed.netloc):
            logger.debug(f"NOK: {url}")
            logger.debug("No valid domain in URL")
            return False

        if not (parsed.path or allow_no_path):
            logger.debug(f"NOK: {url}")
            logger.debug("No path in URL")
            return False

    except Exception:
        logger.debug(f"NOK: {url}")
        logger.debug("URL cannot be parsed by urllib")
        return False

    try:
        url.encode("ascii")
    except Exception:
        logger.debug(f"NOK: {url}")
        logger.debug("URL contains non-ASCII characters")
        return False

    return True

def check_cf(cf_names): # pragma: no cover
    """Check that names are valid CF standard names
    Args:
        cf_names: list of names to test
    Returns:
        True / False
    """
    ok = True
    for cf_name in cf_names:
        try:
            pti.get_cf_standard_name(cf_name)
            logger.debug(f'OK - {cf_name} is a CF standard name.')
        except IndexError:
            logger.debug(f'NOK - {cf_name} is not a CF standard name.')
            ok = False

    return ok

def check_vocabulary(xmldoc): # pragma: no cover
    """Check controlled vocabularies for elements:
        - access_constraint
        - activity_type
        - operational_status (comment: also checked in MMD XSD schema)
        - use_constraint
    Args:
        xmldoc: ElementTree containing the full XML document
    Returns:
        True / False

    Comments: The following elements have test functions available in pythesint but are not used:
    - area -> because it does not correspond to an element in currently tested files
    - platform type -> because erroneous thesaurus in mmd repo?
    """
    vocabularies = {
        'access_constraint': 'access_constraints', 'activity_type': 'activity_type',
        'operational_status': 'operstatus', 'use_constraint': 'use_constraint_type',
    }
    ok = True
    for element_name, f_name in vocabularies.items():
        if f_name == 'use_constraint_type':
            elems_found = xmldoc.findall('./{*}' + element_name + '/{*}identifier')
        else:
            elems_found = xmldoc.findall('./{*}' + element_name)

        if len(elems_found) >= 1:
            for rep in elems_found:
                try:
                    getattr(pti, 'get_mmd_'+f_name)(rep.text)
                    logger.debug(
                        f'OK - {rep.text} is correct vocabulary for element {element_name}.'
                    )
                except IndexError:
                    logger.debug(f'NOK - {rep.text} is not correct vocabulary for element'
                                 f' {element_name}. \n Accepted vocabularies are '
                                 f'{getattr(pti, "get_mmd_"+f_name+"_list")()}')
                    ok = False
        else:
            logger.debug(f'Element {element_name} not present.')

    return ok

def full_check(doc):
    """Main checking scripts for in depth checking of XML file.
     - checking URLs
     - checking lat-lon within geographic_extent/rectangle
     - checking CF names against standard table
     - checking controlled vocabularies (access_constraint /
       activity_type / operational_status / use_constraint)

    Args:
        doc: ElementTree containing the full XML document
    Returns:
        True / False
    """
    valid = True

    # Get elements with urls and check for OK response
    urls = doc.findall(".//{*}resource")
    if len(urls) > 0:
        logger.debug("Checking element(s) containing URL ...")
        urls_ok = all([check_url(elem.text) for elem in urls])
        if urls_ok:
            logger.info("OK: %d URLs" % len(urls))
        else:
            logger.info("NOK: URLs - check debug log")
        valid &= urls_ok
    else:
        logger.debug("Found no elements contained an URL")

    # If there is an element geographic_extent/rectangle, check that lat/lon are valid
    rectangle = doc.findall("./{*}geographic_extent/{*}rectangle")
    if len(rectangle) > 0:
        logger.debug("Checking element geographic_extent/rectangle ...")
        rect_ok = check_rectangle(rectangle)
        if rect_ok:
            logger.info("OK: geographic_extent/rectangle")
        else:
            logger.info("NOK: geographic_extent/rectangle - check debug log")
        valid &= rect_ok
    else:
        logger.debug("Found no geographic_extent/rectangle element")

    # Check that cf name provided exist in reference Standard Name Table
    # cf_elements = doc.findall('./{*}keywords[@vocabulary="Climate and Forecast Standard Names"]')
    # if len(cf_elements) == 1:
    #     logger.debug('Checking elements keyword from vocabulary CF ...')
    #     cf_list = [elem.text for elem in cf_elements[0]]
    #     if len(cf_list) > 1:
    #         logger.info(f'NOK - CF names -> only one CF name should be provided - {cf_list}')
    #         valid = False
    #     # Check CF names even if more than one provided
    #     cf_ok = check_cf(cf_list)
    #     if cf_ok:
    #         logger.info('OK - CF names')
    #     else:
    #         logger.info('NOK - CF names -> check debug log')
    #     valid &= cf_ok
    # elif len(cf_elements) > 1:
    #     valid = False
    #     logger.debug(
    #         'NOK - More than one element with keywords[@vocabulary="Climate and '
    #         'Forecast Standard Names"]'
    #     )
    # else:
    #     logger.debug('No CF standard names element.')

    # Check controlled vocabularies
    # voc_ok = check_vocabulary(doc)
    # valid &= voc_ok
    # if voc_ok:
    #     logger.info('OK - Controlled vocabularies.')
    # else:
    #     logger.info('NOK - Controlled vocabularies -> check debug log')

    return valid

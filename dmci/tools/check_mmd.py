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


class CheckMMD():

    def __init__(self):
        self._status_pass = []
        self._status_fail = []
        self._status_ok = True
        return

    def clear(self):
        """Clear the status data."""
        self._status_pass = []
        self._status_fail = []
        self._status_ok = True
        return

    def status(self):
        """Return the status of checks run since last clear."""
        return self._status_ok, self._status_pass, self._status_fail

    def check_rectangle(self, rectangle):
        """Check if element geographic extent/rectangle is valid:
            - only 1 existing rectangle element
            - rectangle has north / south / west / east subelements
            - -180 <= min_lat <= max_lat <= 180
            -    0 <= min_lon <= max_lon <= 360

        Parameters
        ----------
            rectangle : list of str
                list of elements found when requesting node(s)
                geographic_extent/rectangle

        Returns
        -------
        bool
            True if valid, otherwise False
        """
        keys = ["north", "south", "west", "east"]
        directions = dict.fromkeys(keys, None)

        ok = True
        err = []
        if len(rectangle) > 1:
            err.append("Multiple rectangle elements in file.")
            ok = False

        for child in rectangle[0]:
            child_ns = etree.QName(child)
            tag = child_ns.localname
            if tag not in keys:
                err.append("The element '%s' is not a valid rectangle element." % tag)
                ok = False
            try:
                directions[tag] = float(child.text)
            except ValueError:
                err.append("Value of rectangle element '%s' is not a number." % tag)
                ok = False

        for key, val in directions.items():
            if val is None:
                err.append("Missing rectangle element '%s'." % key)
                ok = False

        if ok:
            # Only check this if all values are successfully read
            if not (-180.0 <= directions["west"] <= directions["east"] <= 180.0):
                err.append("Longitudes not in range -180 <= west <= east <= 180.")
                ok = False

            if not (-90.0 <= directions["south"] <= directions["north"] <= 90.0):
                err.append("Latitudes not in range -90 <= south <= north <= 90.")
                ok = False

        self._log_result("Rectangle Check", ok, err)

        return ok, err

    def check_url(self, url, allow_no_path=False):
        """Check that a URL is valid.

        Parameters
        ----------
        url : str
            The url to check
        allow_no_path : bool, optional
            If True, a url with no path set is considered valid

        Returns
        -------
        bool
            True if valid, otherwise False
        """
        ok = True
        err = []
        try:
            url.encode("ascii")
        except Exception:
            err.append("URL contains non-ASCII characters.")
            ok = False

        try:
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https", "ftp", "sftp"):
                err.append("URL scheme '%s' not allowed." % parsed.scheme)
                ok = False

            if not (parsed.netloc and "." in parsed.netloc):
                err.append("Domain '%s' is not valid." % parsed.netloc)
                ok = False

            if not (parsed.path or allow_no_path):
                err.append("URL contains no path. At least '/' is required.")
                ok = False

        except Exception:
            err.append("URL cannot be parsed by urllib.")
            ok = False

        self._log_result(f"URL Check on '{url}'", ok, err)

        return ok, err

    def check_cf(self, xmldoc):
        """Check that names are valid CF standard names

        Parameters
        ----------
            xmldoc : :obj:`lxml.ElementTree`
                The XML tree to validate

        Returns
        -------
        bool
            True if valid, otherwise False
        """
        ok = True
        err = []

        cf_elements = xmldoc.findall(
            "./{*}keywords[@vocabulary=\"Climate and Forecast Standard Names\"]"
        )
        n_cf = len(cf_elements)
        if n_cf == 1:
            cf_list = [elem.text for elem in cf_elements[0]]
            if len(cf_list) > 1:
                err.append("Only one CF name should be provided, got %d." % len(cf_list))
                ok = False

            # Check CF names even if more than one provided
            for cf_name in cf_list:
                try:
                    pti.get_cf_standard_name(cf_name)
                except IndexError:
                    err.append("Keyword '%s' is not a CF standard name." % cf_name)
                    ok = False

        elif n_cf > 1:
            err.append("More than one CF entry found. Only one is allowed.")
            ok = False

        if n_cf > 0:
            self._log_result("Climate and Forecast Standard Names Check", ok, err)

        return ok, err, n_cf

    # The following function needs to be reimplemented
    def check_vocabulary(self, xmldoc):
        """Check controlled vocabularies for elements:
            - access_constraint
            - activity_type
            - operational_status
            - use_constraint

        Note: The following elements have test functions available
        in pythesint but are not used:
        - area -> because it does not correspond to an element in
        currently tested files
        - platform type -> because erroneous thesaurus in mmd repo?

        Parameters
        ----------
        xmldoc : :obj:`lxml.ElementTree
            XML element containing the full XML document

        Returns
        -------
        ok : bool
            True if valid, otherwise False
        err : list of str
            List of errors
        """
        vocabularies = {
            "access_constraint":  pti.get_mmd_access_constraints,
            "activity_type":      pti.get_mmd_activity_type,
            "operational_status": pti.get_mmd_operstatus,
            "use_constraint":     pti.get_mmd_use_constraint_type,
        }
        ok = True
        err = []
        num = 0

        for element_name, f_name in vocabularies.items():
            if element_name == "use_constraint":
                elems_found = xmldoc.findall("./{*}" + element_name + "/{*}identifier")
            else:
                elems_found = xmldoc.findall("./{*}" + element_name)

            if len(elems_found) >= 1:
                for rep in elems_found:
                    num += 1
                    try:
                        f_name(rep.text)
                    except IndexError:
                        err.append("Incorrect vocabulary '%s' for element '%s'." % (
                            rep.text, element_name
                        ))
                        ok = False

        if num > 0:
            self._log_result("Controlled Vocabularies Check", ok, err)

        return ok, err

    def full_check(self, doc):
        """Main checking scripts for in depth checking of XML file.
            - checking URLs
            - checking lat-lon within geographic_extent/rectangle
            - checking CF names against standard table
            - checking controlled vocabularies (access_constraint /
              activity_type / operational_status / use_constraint)

        Parameters
        ----------
        doc : :obj:`lxml.ElementTree
            XML element containing the full XML document

        Returns
        -------
        bool
            True if valid, otherwise False
        """
        self.clear()
        valid = True

        # Get elements with urls and check for OK response
        urls = doc.findall(".//{*}resource")
        if len(urls) > 0:
            logger.debug("Checking element(s) containing URL ...")
            for elem in urls:
                urls_ok, _ = self.check_url(elem.text)
                valid &= urls_ok

        # If there is an element geographic_extent/rectangle, check that lat/lon are valid
        rectangle = doc.findall("./{*}geographic_extent/{*}rectangle")
        if len(rectangle) > 0:
            logger.debug("Checking element geographic_extent/rectangle ...")
            rect_ok, _ = self.check_rectangle(rectangle)
            valid &= rect_ok

        # Check that cf name provided exist in reference Standard Name Table
        cf_ok, _, _ = self.check_cf(doc)
        valid &= cf_ok

        # Check controlled vocabularies
        voc_ok, _ = self.check_vocabulary(doc)
        valid &= voc_ok

        return valid

    ##
    #  Internal Functions
    ##

    def _log_result(self, check, ok, err):
        """Write the result of a check to the status variables."""
        if ok:
            self._status_pass.append("Passed: %s" % check)
        else:
            self._status_fail.append("Failed: %s" % check)
            for fail in err:
                self._status_fail.append(" - %s" % fail)

        self._status_ok &= ok

        return

# END Class CheckMMD

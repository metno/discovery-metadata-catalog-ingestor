import os
import unittest
import pathlib
from dmci.mmd_tools.check_mmd import check_cf
from dmci.mmd_tools.check_mmd import check_vocabulary, full_check
import lxml.etree as ET

class testMmdCheck(unittest.TestCase):
    def setUp(self):
        #
        # run this test from the root directory by running:
        #
        # python3 -m unittest
        #
        # unset the output limit when printing the xml diff
        #
        current_dir = pathlib.Path.cwd()
        self.reference_xml = os.path.join(current_dir, 'tests', 'files', 'mmd',
                                          'reference_mmd.xml')
        self.reference_xsd = os.path.join(current_dir, 'tests', 'files', 'mmd', 'mmd.xsd')
        self.not_a_file = os.path.join(current_dir, 'tests', 'files', 'mmd', 'not_a_file.xml')
        self.not_a_valid_xml = os.path.join(current_dir, 'tests', 'files', 'mmd',
                                            'not_a_valid_xml.xml')
        self.doc = ET.ElementTree(file=self.reference_xml)
        self.etree_ref = ET.ElementTree(ET.XML(
            "<root>"
            "<a x='123'>https://www.met.no</a>"
            "<geographic_extent>"
            "<rectangle>"
            "<north>76.199661</north>"
            "<south>71.63427</south>"
            "<west>-28.114723</west>"
            "<east>-11.169785</east>"
            "</rectangle>"
            "</geographic_extent>"
            "</root>"))
        self.etree_url_rect_nok = ET.ElementTree(ET.XML(
            "<root>"
            "<a x='123'>https://www.met.not</a>"
            "<geographic_extent>"
            "<rectangle>"
            "<north>76.199661</north>"
            "<south>71.63427</south>"
            "<west>-28.114723</west>"
            "</rectangle>"
            "</geographic_extent>"
            "<keywords vocabulary='Climate and Forecast Standard Names'>"
            "<keyword>sea_surface_temperature</keyword>"
            "<keyword>air_surface_temperature</keyword>"
            "</keywords>"
            "<operational_status>NotOpen</operational_status>"
            "</root>"))
        self.etree_ref_empty = ET.ElementTree(ET.XML(
            "<root>"
            "<a x='123'>'xxx'/><c/><b/></a>"
            "</root>"))

    # Full check
    def test_full_check_1(self):
        self.assertTrue(full_check(self.etree_ref))

    # Full check with no elements to check
    def test_full_check_2(self):
        self.assertTrue(full_check(self.etree_ref_empty))

    # Full check with invalid elements
    def test_full_check_3(self):
        self.assertFalse(full_check(self.etree_url_rect_nok))

    # One real standard name and one fake
    def test_cf_1(self):
        self.assertTrue(check_cf(['sea_surface_temperature']))
        self.assertFalse(check_cf(['sea_surace_temperature']))

    # Twice the element keywords for the same vocabulary
    def test_cf_2(self):
        root = ET.Element("toto")
        key1 = ET.SubElement(root, "keywords", vocabulary='Climate and Forecast Standard Names')
        ET.SubElement(key1, "keyword").text = 'air_temperature'
        key2 = ET.SubElement(root, "keywords", vocabulary='Climate and Forecast Standard Names')
        ET.SubElement(key2, "keyword").text = 'air_temperature'
        self.assertFalse(full_check(root))

    # Correct case
    def test_cf_3(self):
        root = ET.Element("toto")
        root1 = ET.SubElement(root, "keywords", vocabulary='Climate and Forecast Standard Names')
        ET.SubElement(root1, "keyword").text = 'sea_surface_temperature'
        self.assertTrue(full_check(root))

    # Two standard names provided
    def test_cf_4(self):
        root = ET.Element("toto")
        root1 = ET.SubElement(root, "keywords", vocabulary='Climate and Forecast Standard Names')
        ET.SubElement(root1, "keyword").text = 'air_temperature'
        ET.SubElement(root1, "keyword").text = 'sea_surface_temperature'
        self.assertFalse(full_check(root))

    # Test vocabularies
    def test_voc_1(self):
        self.assertTrue(check_vocabulary(ET.ElementTree(ET.XML(
            "<root>"
            "<operational_status>Operational</operational_status>"
            "</root>"))))

    # Test vocabularies
    def test_voc_2(self):
        self.assertFalse(check_vocabulary(ET.ElementTree(ET.XML(
            "<root>"
            "<operational_status>OOperational</operational_status>"
            "</root>"))))

import etddepositor
import pytest
import os
import os.path
import xml.etree.ElementTree as ET
import json

NAMESPACES = {"dc": "http://purl.org/dc/elements/1.1/"}

def test_validate_permissions_document():
    good = """Carleton University Thesis License Agreement||1||Y||24-DEC-20
FIPPA||1||Y||24-DEC-20
Academic Integrity Statement||1||Y||24-DEC-20
LAC Non-Exclusive License||2||Y||20-JAN-21
    """
    etddepositor.validate_permissions_document(good)
    bad_input = """BLAHBLAH"""
    with pytest.raises(etddepositor.UnexpectedLine):
        etddepositor.validate_permissions_document(bad_input)
    # TODO: Test all exceptions

def test_extract_metadata_from_xml_tree():
    title_test = """<thesis xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title xml:lang="en">Error Floor Analysis of Quasi-Cyclic LDPC and Spatially Coupled-LDPC Codes and Construction of Codes with Low Error Floor                                                                                                                                </dc:title>
    <dc:creator>Naseri, Sima </dc:creator>
    <dc:subject>M039</dc:subject>
    <dc:description role="abstract" xml:lang="en">Forward error-correcting (FEC) codes play an important role in transmitting data with extremely high reliability through modern communication systems. In this thesis, we study the design and analysis of low-density parity-check (LDPC) codes in general and spatially coupled (SC) LDPC codes, in particular. At first, we analyze the error floor performance of finite-length protograph-based spatially coupled LDPC codes in terms of their design parameters. We conduct a comprehensive analysis to show that the parameter syndrome former memory plays the main role in the average number of cycles and
    trapping sets in the Tanner graph of finite-length SC-LDPC codes. This, in fact, gives an insight into the error floor performance of protograph-based SC-LDPC codes, and demonstrates the superiority of these codes in the error floor region, compared to their block code counterparts.
    To complement the theoretical analysis conducted in the first stage of this research, we develop corresponding design techniques to construct high-performance quasi-cyclic (QC)-LDPC and SC-LDPC codes. Our design approach is aimed at improving the performance of finite-length (SC) LDPC codes while maintaining the
    decoder complexity and latency small. The improvement in error floor is achieved by minimizing (elimination of) the most harmful trapping set (TS)s. We present two design approaches: 1) imposing simple conditions on the small cycles to eliminate specific classes of trapping sets, 2) developing a search-based design technique such that specific trapping sets are targeted for minimization/elimination. Our constructed QC-LDPC and time-invariant SC-LDPC codes are superior to the state-of-the-art both in terms of their error floor performance and their low decoding latency and
    complexity.
    Finally, we look into the design of finite-length time-invariant QC SC-LDPC codes with a small constraint length and a specific girth. In this respect, different scenarios for the construction process are proposed such that the final QC SC-LDPC code has a specific girth of 6 or 8 with a small constraint length. Bounds on memory and lifting degree are derived accordingly to fulfill the girth constraint associated with the specific scenario. Numerical results are provided to compare with the proposed theoretical bounds.

    </dc:description>
    <dc:publisher country="Canada">Carleton University</dc:publisher>
    <dc:date>2021-01-20</dc:date>
    <dc:type>Electronic Thesis or Dissertation</dc:type>
    <dc:identifier>N/A</dc:identifier>
    <dc:language>eng</dc:language>
    <dc:rights>2 Publicly accessible.</dc:rights>
    <degree>
    <name>Doctor of Philosophy</name>
    <level>2</level>
    <discipline xml:lang="en-us">PHD-82S</discipline>
    <grantor>Carleton University</grantor>
    </degree>
    </thesis>"""
    root = ET.fromstring(title_test)

    data = etddepositor.extract_metadata(root)
    assert data.title == "Error Floor Analysis of Quasi-Cyclic LDPC and Spatially Coupled-LDPC Codes and Construction of Codes with Low Error Floor"

'''
def test_extract_metadata_json():
    with open('sample_output/100983183_4099_output/drupal_import/import.json') as json_output:
        out_data = json.load(json_output)

    with open('sample_input/100983183_4099/data/meta/100983183_4099_etdms_meta.xml') as xml_input:
        in_data = ET.parse(xml_input)
        root = in_data.getroot()

    test_data = etddepositor.extract_metadata(root)
    info = etddepositor.csv_exporter(test_data, "sample_input/100983183_4099/")

    print(test_data.title)

    assert test_data.title == out_data["title"]
    assert test_data.creator == out_data["dcterms_creator"]["und"][0]["value"].strip()
    #assert test_data.description == out_data["dcterms_abstract"]["und"][0]["value"]
    #assert test_data.contributor == out_data["dcterms_contributor"]["und"][0]["second"]
    assert test_data.date == out_data["dcterms_date"]["und"][0]["value"]["date"]
'''

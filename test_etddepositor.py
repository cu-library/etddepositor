import etddepositor
from etddepositor import ETDPackageData
import pytest
import yaml
import xml.etree.ElementTree as ET

NAMESPACES = {"dc": "http://purl.org/dc/elements/1.1/"}

valid_document = """Carleton University Thesis License Agreement||1||Y||24-DEC-20
FIPPA||1||Y||24-DEC-20
Academic Integrity Statement||1||Y||24-DEC-20
LAC Non-Exclusive License||2||Y||20-JAN-21
"""

valid_document_two = """Embargo Expiry: 19-APR-21
Carleton University Thesis License Agreement||1||Y||24-DEC-20
FIPPA||1||Y||24-DEC-20
Academic Integrity Statement||1||Y||24-DEC-20
LAC Non-Exclusive License||2||Y||20-JAN-21
"""

not_signed = """Carleton University Thesis License Agreement||1||Y||24-DEC-20
FIPPA||1||Y||24-DEC-20
Academic Integrity Statement||1||N||24-DEC-20
LAC Non-Exclusive License||2||Y||20-JAN-21
"""

embargo_date = """Embargo Expiry: 19-DEC-21
Carleton University Thesis License Agreement||1||Y||24-DEC-20
FIPPA||1||Y||24-DEC-20
Academic Integrity Statement||1||Y||24-DEC-20
LAC Non-Exclusive License||2||Y||20-JAN-21
"""

PACKAGE_DATA = ETDPackageData(
    source_identifier="100692623_894",
    title="Permafrost and Thermokarst Lake Dynamics in the Old Crow Flats, Northern Yukon, Canada",
    creator="Roy-Leveillee, Pascale",
    pro_subject="Physical Geography | Environmental Sciences | Geography",
    lc_subject=[
        [["a", "Physical geography."]],
        [["a", "Environmental sciences."]],
        [["a", "Geography."]],
    ],
    description="Aspects of the thaw lake cycle were investigated in Old Crow Flats (OCF), a 5600 km2 peatland with thousands of thermokarst lakes in the continuous permafrost of northern Yukon. It is located in the traditional territory of the Vuntut Gwitch'n, who expressed concern that climatic change may be affecting the permafrost and lakes of OCF.      Field data collected in 2008-2011 provided the first assessment of spatial variability in permafrost temperatures across the treeline ecotone in OCF. Lake-bottom temperatures were recorded near the shores of four thermokarst lakes and talik     configuration was defined beneath the lakes by jet-drilling to determine conditions controlling permafrost degradation in the area. Analytical and thermal models were used to relate field observations to current theory. Surface and subsurface conditions were examined in three drained lake basins and four expanding lakes to investigate how shore recession, talik development, and sediment deposition during lake expansion control the topography in lake basins after drainage.      Permafrost temperature at the depth of zero annual amplitude varied between -5.1ºC and -2.6ºC on the Flats.     Within the forest-tundra transition, spatial variability in permafrost temperatures appeared to be controlled by the snow-holding capacity of vegetation and the configuration of land covers in the surrounding landscape, which controlled snow supply. Annual mean lake-bottom temperatures close to shorelines were unaffected by spatial variations in on-ice snow depth, but accumulation of freezing degree-days at the lake bottom varied sufficiently to affect rates of permafrost degradation beneath the lake. Where ice reached the lake bottom, talik development rates were controlled by the ratio of     freezing degree days to thawing degree days and the thermal offset in the lake sediment. After lake drainage and permafrost aggradation, thermokarst lake basins in OCF commonly develop depressed margins and raised centres. An elevation difference of up to 2 m was recorded between the margins and centres of drained basins, but this elevation difference was not associated with increased ice-wedge density or increased segregated ice content.  A conceptual model based on sediment deposition patterns during lake expansion was proposed to explain the topography of drained lake basins in OCF.",
    publisher="Carleton University",
    contributor="Christopher R. Burn | Ian D. McDonald",
    date_created="2014",
    language="English",
    name="Doctor of Philosophy",
    discipline="Geography",
    level="Doctoral",
    resource_type="Dissertation",
)

log_success_array = []


# @pytest.mark.parametrize("documents", [(valid_document), (valid_document_two), (not_signed), (embargo_date)])
@pytest.mark.parametrize("documents", [(valid_document), (valid_document_two)])
def test_validate_permissions_document_one(documents):

    etddepositor.validate_permissions_document(documents)


def test_extract_metadata_from_xml_tree():
    title_test = """<thesis xmlns="http://www.ndltd.org/standards/metadata/etdms/1.1/"
                                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                        xmlns:dc="http://purl.org/dc/elements/1.1/"
                                        xmlns:dcterms="http://purl.org/dc/terms/"
                                        xsi:schemaLocation="http://www.ndltd.org/standards/metadata/etdms/1.1/
                                        http://www.ndltd.org/standards/metadata/etdms/1.1/etdms11.xsd
                                        http://purl.org/dc/elements/1.1/
                                        http://www.ndltd.org/standards/metadata/etdms/1.1/etdmsdc.xsd
                                        http://purl.org/dc/terms/
                                        http://www.ndltd.org/standards/metadata/etdms/1.1/etdmsdcterms.xsd"
                                        >
    <dc:title xml:lang="en">Permafrost and Thermokarst Lake Dynamics in the Old Crow Flats, Northern Yukon, Canada</dc:title>
    <dc:creator>Roy-Leveillee, Pascale </dc:creator>
    <dc:subject>R014</dc:subject>
    <dc:subject>H001</dc:subject>
    <dc:subject>S022</dc:subject>
    <dc:description role="abstract" xml:lang="en">
    Aspects of the thaw lake cycle were investigated in Old Crow Flats (OCF), a 5600 km2 peatland with thousands of thermokarst lakes in the continuous permafrost of northern Yukon. It is located in the traditional territory of the Vuntut Gwitch&apos;n, who expressed concern that climatic change may be affecting the permafrost and lakes of OCF.

    Field data collected in 2008-2011 provided the first assessment of spatial variability in permafrost temperatures across the treeline ecotone in OCF. Lake-bottom temperatures were recorded near the shores of four thermokarst lakes and talik
    configuration was defined beneath the lakes by jet-drilling to determine conditions controlling permafrost degradation in the area. Analytical and thermal models were used to relate field observations to current theory. Surface and subsurface conditions were examined in three drained lake basins and four expanding lakes to investigate how shore recession, talik development, and sediment deposition during lake expansion control the topography in lake basins after drainage.

    Permafrost temperature at the depth of zero annual amplitude varied between -5.1ºC and -2.6ºC on the Flats.
    Within the forest-tundra transition, spatial variability in permafrost temperatures appeared to be controlled by the snow-holding capacity of vegetation and the configuration of land covers in the surrounding landscape, which controlled snow supply. Annual mean lake-bottom temperatures close to shorelines were unaffected by spatial variations in on-ice snow depth, but accumulation of freezing degree-days at the lake bottom varied sufficiently to affect rates of permafrost degradation beneath the lake. Where ice reached the lake bottom, talik development rates were controlled by the ratio of
    freezing degree days to thawing degree days and the thermal offset in the lake sediment. After lake drainage and permafrost aggradation, thermokarst lake basins in OCF commonly develop depressed margins and raised centres. An elevation difference of up to 2 m was recorded between the margins and centres of drained basins, but this elevation difference was not associated with increased ice-wedge density or increased segregated ice content.  A conceptual model based on sediment deposition patterns during lake expansion was proposed to explain the topography of drained lake basins in OCF.

    </dc:description>
    <dc:publisher country="Canada">Carleton University</dc:publisher>
    <dc:contributor role="Supervisor">Christopher R. Burn</dc:contributor>
    <dc:contributor role="Northern Research Partner">Ian D. McDonald</dc:contributor>
    <dc:date>2014-12-22</dc:date>
    <dc:type>Electronic Thesis or Dissertation</dc:type>
    <dc:identifier>N/A</dc:identifier>
    <dc:language>eng</dc:language>
    <dc:rights>2 Publicly accessible.</dc:rights>
    <degree>
    <name>Doctor of Philosophy</name>
    <level>2</level>
    <discipline xml:lang="en-us">PHD-42</discipline>
    <grantor>Carleton University</grantor>
    </degree>
    </thesis>"""

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)
    root = ET.fromstring(title_test)
    data = etddepositor.extract_metadata(root, "100692623_894", config_yaml)

    assert (
        data.title
        == "Permafrost and Thermokarst Lake Dynamics in the Old Crow Flats, Northern Yukon, Canada"
    )


def test_crossref(tmp_path):
    crossref_dir = tmp_path / "crossref"
    crossref_dir.mkdir()

    # Naming of XML files for DOIs
    running_file = crossref_dir / "running.xml"
    crossref_file = crossref_dir / "crossref.xml"

    work_link = {}
    work_link["100692623_894"] = "localhost:3000/concern/works/example"
    identifier = 10000

    crossref_data = etddepositor.create_crossref_data(
        PACKAGE_DATA, identifier, work_link
    )

    assert crossref_data.given_name == "Pascale"
    assert crossref_data.surname == "Roy-Leveillee"

    # Create crossref entry, return doi link for created entry
    doi_link = etddepositor.create_crossref(crossref_data, crossref_file, running_file)

    assert doi_link == "10.22215/etd/2014-10000"


def test_marc_record(tmp_path):

    marc_dir = tmp_path / "marc"
    marc_dir.mkdir()
    work_link = "localhost:3000/concern/works/example"

    etddepositor.create_marc_record(
        "100692623_894",
        marc_dir,
        work_link,
        PACKAGE_DATA,
    )

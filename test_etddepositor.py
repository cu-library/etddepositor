import csv
import xml.etree.ElementTree as ElementTree
import datetime
import pymarc
import pytest

import etddepositor


def test_write_metadata_csv_header(tmp_path):
    metadata_csv_path = tmp_path / "metadata.csv"
    etddepositor.write_metadata_csv_header(metadata_csv_path)
    assert metadata_csv_path.read_text() == (
        "source_identifier,model,title,creator,identifier,subject,"
        "abstract,publisher,contributor,date_created,language,agreement,"
        "degree,degree_discipline,degree_level,resource_type,parents,"
        "file,rights_notes\n"
    )


class TestEmbargoAndAgreements:

    mappings = {
        "agreements": {
            "Academic Integrity Statement": {
                "identifier": "ais",
                "required": True,
            },
            "Carleton University Thesis License Agreement": {
                "identifier": "cutla",
                "required": True,
            },
            "FIPPA": {
                "identifier": "fs",
                "required": True,
            },
            "LAC Non-Exclusive License": {
                "identifier": "lnel",
                "required": False,
            },
        }
    }

    valid = """Student ID: 10000000
Thesis ID: 1000
Embargo Expiry: 13-AUG-16
Carleton University Thesis License Agreement||1||Y||06-AUG-15
FIPPA||1||Y||06-AUG-15
Academic Integrity Statement||1||Y||06-AUG-15
LAC Non-Exclusive License||2||Y||31-AUG-15
"""

    valid_no_lac = """Student ID: 10000000
Thesis ID: 1000
Carleton University Thesis License Agreement||1||Y||06-AUG-15
FIPPA||1||Y||06-AUG-15
Academic Integrity Statement||1||Y||06-AUG-15
LAC Non-Exclusive License||2||N||31-AUG-15
"""

    not_signed = """Student ID: 10000000
Thesis ID: 1000
Carleton University Thesis License Agreement||1||N||06-AUG-15
FIPPA||1||Y||06-AUG-15
Academic Integrity Statement||1||Y||06-AUG-15
LAC Non-Exclusive License||2||N||31-AUG-15
"""

    weird_line = """BOO!"""

    embargo_date_not_passed = """Student ID: 100944645
Thesis ID: 1794
Embargo Expiry: 13-AUG-99
Carleton University Thesis License Agreement||1||Y||19-APR-16
FIPPA||1||Y||19-APR-16
Academic Integrity Statement||1||Y||19-APR-16
LAC Non-Exclusive License||2||Y||13-MAY-16
"""

    embargo_date_bad = """Student ID: 100944645
Thesis ID: 1794
Embargo Expiry: Epoch+1
Carleton University Thesis License Agreement||1||Y||19-APR-16
FIPPA||1||Y||19-APR-16
Academic Integrity Statement||1||Y||19-APR-16
LAC Non-Exclusive License||2||Y||13-MAY-16
"""

    def test_valid(self):
        assert etddepositor.process_embargo_and_agreements(
            self.valid.strip().split("\n"), self.mappings
        ) == ["cutla", "fs", "ais", "lnel"]

    def test_valid_no_lac(self):
        assert etddepositor.process_embargo_and_agreements(
            self.valid_no_lac.strip().split("\n"), self.mappings
        ) == ["cutla", "fs", "ais"]

    def test_not_signed(self):
        with pytest.raises(
            etddepositor.MetadataError,
            match=r"Carleton.* is required but not signed",
        ):
            etddepositor.process_embargo_and_agreements(
                self.not_signed.strip().split("\n"), self.mappings
            )

    def test_weird_line(self):
        with pytest.raises(
            etddepositor.MetadataError, match=r"BOO! was not expected.*"
        ):
            etddepositor.process_embargo_and_agreements(
                self.weird_line.strip().split("\n"), self.mappings
            )

    def test_embargo_date_not_passed(self):
        with pytest.raises(
            etddepositor.MetadataError, match=r"the embargo date of.*2099"
        ):
            etddepositor.process_embargo_and_agreements(
                self.embargo_date_not_passed.strip().split("\n"), self.mappings
            )

    def test_embargo_date_bad(self):
        with pytest.raises(
            etddepositor.MetadataError, match="could not be processed"
        ):
            etddepositor.process_embargo_and_agreements(
                self.embargo_date_bad.strip().split("\n"), self.mappings
            )


def test_create_package_data():
    today = datetime.date.today().year
    mappings = {
        "abbreviation": {"Doctor of Philosophy": "Ph.D."},
        "discipline": {"PHD-01": "Processing Studies"},
        "lc_subject": {
            "CODE1": [["a", "TestCode1."], ["a", "Test2", "x", "Specify"]],
            "CODE2": [["a", "TestCode2."]],
        },
    }
    package_metadata_xml = ElementTree.ElementTree(
        ElementTree.fromstring(
            """<thesis
xmlns="http://www.ndltd.org/standards/metadata/etdms/1.1/"
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
  <dc:title xml:lang="en">Title</dc:title>
  <dc:creator>Creator, Test</dc:creator>
  <dc:subject>CODE1</dc:subject>
  <dc:subject>CODE2</dc:subject>
  <dc:description role="abstract" xml:lang="en">
    \u00E9Abstract\n\r
  </dc:description>
  <dc:publisher>Publisher</dc:publisher>
  <dc:contributor role="co-author">Contributor A</dc:contributor>
  <dc:contributor>Contributor B</dc:contributor>
  <dc:date>2021-01-01</dc:date>
  <dc:type>Electronic Thesis or Dissertation</dc:type>
  <dc:language>fre</dc:language>
  <degree>
    <name>Doctor of Philosophy</name>
    <level>2</level>
    <discipline>PHD-01</discipline>
    <grantor>Carleton University</grantor>
  </degree>
</thesis>"""
        )
    )
    assert etddepositor.create_package_data(
        package_metadata_xml,
        "StudentNumber_ThesisNumber",
        77,
        ["agreement_one", "agreement_two"],
        "/a/path/here",
        mappings,
    ) == etddepositor.PackageData(
        name="StudentNumber_ThesisNumber",
        source_identifier=(
            "8fa99d4e9e189018f4781a5549d0f092"
            "616664c2d15403c4a83b3d62b967719d"
        ),
        title="Title",
        creator="Creator, Test",
        subjects=[
            ["a", "TestCode1."],
            ["a", "Test2", "x", "Specify"],
            ["a", "TestCode2."],
        ],
        abstract="\u00E9Abstract",
        publisher="Publisher",
        contributors=["Contributor A (Co-author)|||Contributor B"],
        date="2021",
        language="fra",
        agreements=["agreement_one|||agreement_two"],
        degree="Doctor of Philosophy",
        abbreviation="Ph.D.",
        discipline="Processing Studies",
        level="2",
        url="",
        doi=f"{etddepositor.DOI_PREFIX}/etd/2021-77",
        path="/a/path/here",
        rights_notes=(
            f"Copyright © {today} the author(s). Theses may be used for "
            "non-commercial research, educational, or related academic "
            "purposes only. Such uses include personal study, distribution to"
            " students, research and scholarship. Theses may only be shared by"
            " linking to Carleton University Digital Library and no part may "
            "be copied without proper attribution to the author; no part may "
            "be used for commercial purposes directly or indirectly via a "
            "for-profit platform; no adaptation or derivative works are "
            "permitted without consent from the copyright owner."
        ),
    )

    empty_package_metadata_xml = ElementTree.ElementTree(
        ElementTree.fromstring(
            """<thesis
xmlns="http://www.ndltd.org/standards/metadata/etdms/1.1/"
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
</thesis>"""
        )
    )

    with pytest.raises(
        etddepositor.MetadataError, match="title tag is missing"
    ):
        etddepositor.create_package_data(
            empty_package_metadata_xml,
            "StudentNumber_ThesisNumber",
            1,
            [],
            "/a/path/here",
            mappings,
        )

    empty_with_title_package_metadata_xml = ElementTree.ElementTree(
        ElementTree.fromstring(
            """<thesis
xmlns="http://www.ndltd.org/standards/metadata/etdms/1.1/"
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
  <dc:title xml:lang="en">Title</dc:title>
</thesis>"""
        )
    )

    with pytest.raises(
        etddepositor.MetadataError, match="creator tag is missing"
    ):
        etddepositor.create_package_data(
            empty_with_title_package_metadata_xml,
            "StudentNumber_ThesisNumber",
            1,
            [],
            "/a/path/here",
            mappings,
        )


def test_process_subjects():
    mappings = {
        "lc_subject": {
            "B001": [["a", "Agriculture."]],
            "B013": [
                ["a", "Wood."],
                ["a", "Forest products.", "x", "Biotechnology"],
            ],
        }
    }

    subject_elements = [
        ElementTree.Element("subject"),
        ElementTree.Element("subject"),
        ElementTree.Element("subject"),
        ElementTree.Element("subject"),
        ElementTree.Element("subject"),
    ]

    subject_elements[0].text = "  B001"
    subject_elements[1].text = "B013  "
    subject_elements[2].text = "Unknown"
    subject_elements[3].text = "B001 "
    subject_elements[4].text = " B013 "

    assert etddepositor.process_subjects(subject_elements, mappings) == [
        ["a", "Agriculture."],
        ["a", "Wood."],
        ["a", "Forest products.", "x", "Biotechnology"],
    ]


def test_process_description():
    assert (
        etddepositor.process_description("   \n\r   Abstract!\n  \n\r")
        == "Abstract!"
    )


def test_process_contributors():
    contributor_no_role = ElementTree.Element("contributor")
    contributor_no_role.text = "Kevin Bowrin"
    contributor_with_role = ElementTree.Element("contributor")
    contributor_with_role.text = "James Ronin"
    contributor_with_role.set("role", "co-author")

    assert etddepositor.process_contributors(
        [contributor_no_role, contributor_with_role]
    ) == ["Kevin Bowrin", "James Ronin (Co-author)"]


def test_process_date():
    assert etddepositor.process_date("2021-06-01") == "2021"
    assert etddepositor.process_date("1900-06-01") == "1900"
    with pytest.raises(etddepositor.MetadataError, match="missing"):
        etddepositor.process_date("")
    with pytest.raises(
        etddepositor.MetadataError, match="not properly formatted"
    ):
        etddepositor.process_date("BLAH")


def test_process_language():
    assert etddepositor.process_language("fre") == "fra"
    assert etddepositor.process_language("fra") == "fra"
    assert etddepositor.process_language("ger") == "deu"
    assert etddepositor.process_language("deu") == "deu"
    assert etddepositor.process_language("spa") == "spa"
    assert etddepositor.process_language("eng") == "eng"
    assert etddepositor.process_language("") == "eng"
    with pytest.raises(
        etddepositor.MetadataError, match="unexpected language"
    ):
        etddepositor.process_language("bla")


def test_process_degree():
    assert etddepositor.process_degree("Master of Stuff") == "Master of Stuff"
    assert (
        etddepositor.process_degree(" Master of Stuff ") == "Master of Stuff"
    )
    assert (
        etddepositor.process_degree("Master of Architectural Stud")
        == "Master of Architectural Studies"
    )
    assert (
        etddepositor.process_degree("Master of Information Tech")
        == "Master of Information Technology"
    )
    assert etddepositor.process_degree("") == etddepositor.FLAG


def test_process_degree_abbreviation():
    mappings = {
        "abbreviation": {
            "Doctor of Philosophy": "Ph.D.",
        }
    }

    assert (
        etddepositor.process_degree_abbreviation(
            "Doctor of Philosophy", mappings
        )
        == "Ph.D."
    )

    assert (
        etddepositor.process_degree_abbreviation("Unknown", mappings)
        == etddepositor.FLAG
    )


def test_process_degree_discipline():
    mappings = {
        "discipline": {
            "MA-07": "Communication",
            "MA-15": "English",
        }
    }

    assert (
        etddepositor.process_degree_discipline("MA-07", mappings)
        == "Communication"
    )

    assert (
        etddepositor.process_degree_discipline("   MA-15   ", mappings)
        == "English"
    )

    assert (
        etddepositor.process_degree_discipline("Unknown", mappings)
        == etddepositor.FLAG
    )


def test_process_degree_level():
    assert etddepositor.process_degree_level("1") == "1"
    assert etddepositor.process_degree_level("2") == "2"
    with pytest.raises(
        etddepositor.MetadataError, match="degree level is missing"
    ):
        etddepositor.process_degree_level("")
    with pytest.raises(etddepositor.MetadataError, match="undergraduate work"):
        etddepositor.process_degree_level("0")
    with pytest.raises(
        etddepositor.MetadataError, match="invalid degree level"
    ):
        etddepositor.process_degree_level("blah")


def test_add_to_csv(tmp_path):
    metadata_csv_path = tmp_path / "metadata.csv"
    package_data = etddepositor.PackageData(
        name="StudentNumber_ThesisNumber",
        source_identifier=(
            "8fa99d4e9e189018f4781a5549d0f092"
            "616664c2d15403c4a83b3d62b967719d"
        ),
        title="Title",
        creator="Creator, Test",
        subjects=[
            ["a", "TestCode1."],
            ["a", "Test2", "x", "Specify"],
            ["a", "TestCode2."],
        ],
        abstract="\u00E9Abstract",
        publisher="Publisher",
        contributors=["Contributor A (Co-author)|||Contributor B"],
        date="2021",
        language="fra",
        agreements=["agreement_one|||agreement_two"],
        degree="Doctor of Philosophy",
        abbreviation="Ph.D.",
        discipline="Processing Studies",
        level="2",
        url="",
        doi=f"{etddepositor.DOI_PREFIX}/etd/2021-77",
        path="/a/path/here",
        rights_notes="",
    )

    etddepositor.add_to_csv(
        metadata_csv_path,
        package_data,
        "collection_id_1",
        ["/tmp/file1|||/tmp/file2"],
    )

    with open(
        metadata_csv_path, newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_reader = csv.reader(metadata_csv_file)
        line = next(csv_reader)
        assert line == [
            (
                "8fa99d4e9e189018f4781a5549d0f092"
                "616664c2d15403c4a83b3d62b967719d"
            ),
            "Etd",
            "Title",
            "Creator, Test",
            (
                "DOI: "
                f"{etddepositor.DOI_URL_PREFIX}"
                f"{etddepositor.DOI_PREFIX}/etd/2021-77"
            ),
            "TestCode1|Test2 -- Specify|TestCode2",
            "\u00E9Abstract",
            "Publisher",
            "Contributor A (Co-author)|||Contributor B",
            "2021",
            "fra",
            "agreement_one|||agreement_two",
            "Doctor of Philosophy (Ph.D.)",
            "Processing Studies",
            "2",
            "Thesis",
            "collection_id_1",
            "/tmp/file1|||/tmp/file2",
            "",
        ]


def test_create_csv_subject():
    assert etddepositor.create_csv_subject([["a", "Physics."]]) == "Physics"
    assert (
        etddepositor.create_csv_subject(
            [["a", "Physics.", "x", "Alternative."]]
        )
        == "Physics -- Alternative"
    )
    assert (
        etddepositor.create_csv_subject(
            [["a", "Mathematics."], ["a", "Chemistry."]]
        )
        == "Mathematics|Chemistry"
    )


def test_create_marc_record(tmp_path):
    etddepositor.create_marc_record(
        etddepositor.PackageData(
            name="StudentNumber_ThesisNumber",
            source_identifier="",
            title="Title",
            creator="Creator, Test",
            subjects=[
                ["a", "TestCode1."],
                ["a", "Test2.", "x", "Specify."],
                ["a", "TestCode2."],
            ],
            abstract="",
            publisher="",
            contributors=[],
            date="2021",
            language="fra",
            agreements=[],
            degree="",
            abbreviation="Ph.D.",
            discipline="Processing Studies",
            level="",
            url="",
            doi="10.223/etd/2021-1",
            path="",
            rights_notes="test notes",
        ),
        tmp_path,
    )

    with open(
        tmp_path / "StudentNumber_ThesisNumber_marc.mrc", "rb"
    ) as marc_file:
        record = next(pymarc.MARCReader(marc_file, to_unicode=True))
        assert record.title() == "Title."
        assert record["100"]["a"] == "Creator, Test,"
        assert (
            record["502"]["a"] == "Thesis (Ph.D.) - Carleton University, 2021."
        )
        assert [f.subfields_as_dict() for f in record.get_fields("650")] == [
            {"a": ["TestCode1."]},
            {"a": ["Test2."], "x": ["Specify."]},
            {"a": ["TestCode2."]},
        ]
        assert record["710"]["g"] == "Processing Studies."
        assert (
            record["856"]["u"]
            == f"{etddepositor.DOI_URL_PREFIX}10.223/etd/2021-1"
        )


def test_create_crossref_etree():
    test_crossref_etree, body = etddepositor.create_crossref_etree()
    root = test_crossref_etree.getroot()
    assert root.tag == "doi_batch"
    assert (
        root.findtext("head/depositor/depositor_name")
        == "Carleton University Library"
    )
    assert (
        root.findtext("head/depositor/email_address")
        == "doi@library.carleton.ca"
    )


def test_create_dissertation_element():
    dissertation_element = etddepositor.create_dissertation_element(
        etddepositor.PackageData(
            name="",
            source_identifier="",
            title="Title",
            creator="Creator, Test",
            subjects=[],
            abstract="",
            publisher="",
            contributors=[],
            date="2021",
            language="",
            agreements=[],
            degree="Doctor of Philosophy",
            abbreviation="",
            discipline="",
            level="",
            url="https://a.url.here/work1",
            doi=f"{etddepositor.DOI_PREFIX}/etd/2021-1",
            path="",
            rights_notes="test notes",
        )
    )
    assert dissertation_element.findtext("person_name/given_name") == "Test"
    assert dissertation_element.findtext("person_name/surname") == "Creator"
    assert dissertation_element.findtext("titles/title") == "Title"
    assert dissertation_element.findtext("approval_date/year") == "2021"
    assert dissertation_element.findtext("degree") == "Doctor of Philosophy"
    assert (
        dissertation_element.findtext("doi_data/doi")
        == f"{etddepositor.DOI_PREFIX}/etd/2021-1"
    )
    assert (
        dissertation_element.findtext("doi_data/resource")
        == "https://a.url.here/work1"
    )

    dissertation_element_mononymous = etddepositor.create_dissertation_element(
        etddepositor.PackageData(
            name="",
            source_identifier="",
            title="",
            creator="Mononymous",
            subjects=[],
            abstract="",
            publisher="",
            contributors=[],
            date="",
            language="",
            agreements=[],
            degree="",
            abbreviation="",
            discipline="",
            level="",
            url="",
            doi="",
            path="",
            rights_notes="",
        )
    )
    assert (
        dissertation_element_mononymous.findtext("person_name/given_name")
        == ""
    )
    assert (
        dissertation_element_mononymous.findtext("person_name/surname")
        == "Mononymous"
    )

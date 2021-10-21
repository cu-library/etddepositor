import etddepositor
import pytest


def test_cwrite_metadata_csv_header(tmp_path):
    metadata_csv_path = tmp_path / "metadata.csv"
    etddepositor.write_metadata_csv_header(metadata_csv_path)
    assert metadata_csv_path.read_text() == (
        "source_identifier,model,title,creator,identifier,subject,"
        "abstract,publisher,contributor,date_created,language,"
        "degree,degree_discipline,degree_level,resource_type,collection,file\n"
    )


class TestEmbargoAndAgreements:

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

    @pytest.mark.parametrize("document", [valid, valid_no_lac])
    def test_check_embargo_and_agreements_pass(self, document):
        etddepositor.check_embargo_and_agreements(document.strip().split("\n"))

    def test_not_signed(self):
        with pytest.raises(
            etddepositor.MetadataError, match=r"Carleton.* is invalid"
        ):
            etddepositor.check_embargo_and_agreements(
                self.not_signed.strip().split("\n")
            )

    def test_weird_line(self):
        with pytest.raises(
            etddepositor.MetadataError, match=r"BOO! was not expected.*"
        ):
            etddepositor.check_embargo_and_agreements(
                self.weird_line.strip().split("\n")
            )

    def test_embargo_date_not_passed(self):
        with pytest.raises(
            etddepositor.MetadataError, match=r"the embargo date of.*2099"
        ):
            etddepositor.check_embargo_and_agreements(
                self.embargo_date_not_passed.strip().split("\n")
            )

    def test_embargo_date_bad(self):
        with pytest.raises(
            etddepositor.MetadataError, match="could not be processed"
        ):
            etddepositor.check_embargo_and_agreements(
                self.embargo_date_bad.strip().split("\n")
            )


def test_process_date():
    assert etddepositor.process_date("2021-06-01") == "2021"
    assert etddepositor.process_date("1900-06-01") == "1900"
    with pytest.raises(etddepositor.MetadataError, match="missing"):
        etddepositor.process_date("")
    with pytest.raises(
        etddepositor.MetadataError, match="not properly formatted"
    ):
        etddepositor.process_date("BLAH")

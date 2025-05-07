import os
import glob
import datetime
import yaml
import click
import xml.etree.ElementTree as ElementTree
import shutil
import csv
import bagit
import dataclasses
import string
from typing import List
import ast
from xml.etree import ElementTree
from xml.dom import minidom
import subprocess


READY_SUBDIR = "ready"
DONE_SUBDIR = "done"
MARC_SUBDIR = "marc"
CROSSREF_SUBDIR = "crossref"
CSV_REPORT_SUBDIR = "csv_report"
DSPACE_SAF_OUTPUT_SUBDIR = "dspace_saf"
NOT_COMPLETE_SUBDIR = "not_complete"
ITEM_SUBDIR_PREFIX = "item_"
DUBLIN_CORE_FILENAME = "dublin_core.xml"
CONTENTS_FILENAME = "contents"
NAMESPACES = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "etdms": "http://www.ndltd.org/standards/metadata/etdms/1.1/",
}

PackageData = dataclasses.make_dataclass(
    "PackageData",
    [
        "package_files", 
        "creator", 
        "contributors",
        "date",
        "type",
        "description",
        "publisher",
        "doi",
        "language",
        "rights_notes",
        "title",
        "subjects",
        "agreements",
        "degree",
        "degree_discipline",
        "degree_level", 
    ],
)

# DOI_PREFIX is Carleton University Library's DOI prefix, used when minting new
# DOIs for ETDs.
DOI_PREFIX = "10.22215"

# FLAG is a string which we assign to some attributes of the package
# if our mapping for that attribute is incomplete or unknowable.
FLAG = "FLAG"

class MissingFileError(Exception):
    """Raised when a required file is missing."""


class MetadataError(Exception):
    """Raised when a problem with the package metadata is encountered."""


class GetURLFailedError(Exception):
    """Raised when the Hyrax URL for an imported package can't be found."""

def load_mappings(mapping_file):
    """Loads the mappings YAML file."""
    try:
        with open(mapping_file, encoding="utf-8") as mappings_file:
            mappings = yaml.load(mappings_file, Loader=yaml.FullLoader)
        return mappings
    except FileNotFoundError:
        click.echo(f"Error: Mappings file not found at {mapping_file}")
        return None
    except yaml.YAMLError as e:
        click.echo(f"Error parsing mappings file: {e}")
        return None

def validate_subject_mappings(mappings):
    """Ensures the subjects in the mappings file are properly formatted."""
    if mappings and "lc_subject" in mappings:
        for code, subject in mappings["lc_subject"].items():
            for subject_tags in subject:
                if len(subject_tags) not in [2, 4]:
                    click.echo(f"Warning: The subject {code} in the mappings file is not formatted correctly.")

def find_etd_packages(processing_directory):
    """Finds the package directories in the ready subdirectory."""
    ready_path = os.path.join(processing_directory, READY_SUBDIR)
    packages = glob.glob(os.path.join(ready_path, "*"))
    return packages

def create_output_directories(processing_directory):
    """Creates the timestamped output directories."""
    done_path = os.path.join(processing_directory, DONE_SUBDIR)
    dspace_saf_output_path = os.path.join(processing_directory, DSPACE_SAF_OUTPUT_SUBDIR)
    marc_path = os.path.join(processing_directory, MARC_SUBDIR)
    crossref_path = os.path.join(processing_directory, CROSSREF_SUBDIR)
    csv_report_path = os.path.join(processing_directory, CSV_REPORT_SUBDIR)
    not_complete_path = os.path.join(processing_directory, NOT_COMPLETE_SUBDIR)

    os.makedirs(done_path, mode=0o770, exist_ok=True)
    os.makedirs(marc_path, mode=0o775, exist_ok=True)
    os.makedirs(crossref_path, mode=0o775, exist_ok=True)
    os.makedirs(csv_report_path, mode=0o775, exist_ok=True)
    os.makedirs(dspace_saf_output_path, mode=0o775, exist_ok=True)
    os.makedirs(not_complete_path, mode=0o775, exist_ok=True)

    return done_path, marc_path, crossref_path, csv_report_path, dspace_saf_output_path

def write_metadata_csv_header(metadata_csv_path):
    """Write the header columns to the Hyrax import metadata CSV file."""
    header_columns = [
        "files", 
        "dc.contributor.author", 
        "dc.contributor.other",
        "dc.date.issued",
        "dc.type",
        "dc.description.abstract",
        "dc.publisher",
        "dc.identifier.doi",
        "dc.language.iso",
        "dc.rights",
        "dc.title",
        "dc.subject.lcsh"
    ]

    with open(
        metadata_csv_path, "w", newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_writer = csv.writer(metadata_csv_file)

        csv_writer.writerow(header_columns)


def process_subjects(subject_elements, mappings):
    subjects = []
    for subject_element in subject_elements:
        subject_code = subject_element.text.strip()
        if subject_code in mappings["lc_subject"]:
            for subject_tags in mappings["lc_subject"][subject_code]:
                subjects.append(subject_tags)
    deduplicated_subjects = []
    for subject in subjects:
        if subject not in deduplicated_subjects:
            deduplicated_subjects.append(subject)
    return deduplicated_subjects

def process_description(description):
    return description.replace("\n", " ").replace("\r", "").strip()

def process_contributors(contributor_elements):
    contributors = []
    for contributor_element in contributor_elements:
        name = contributor_element.text.strip()
        role = contributor_element.get("role", "")
        if role:
            # Uppercase the first character of the role.
            role = role[0].upper() + role[1:]
            contributors.append(f"{name} ({role})")
        else:
            contributors.append(name)
    return contributors

def process_date(date):
    """Check date is properly formatted, return the date and year as strings"""

    date = date.strip()
    if not date:
        raise MetadataError("date tag is missing")
    try:
        year = str(datetime.datetime.strptime(date, "%Y-%m-%d").year)
    except ValueError:
        raise MetadataError(f"date value {date} is not properly formatted")
    return date, year

def process_language(language):
    language = language.strip()
    if language == "fre" or language == "fra":
        return "fr"
    elif language == "ger" or language == "deu":
        return "de"
    elif language == "spa":
        return "sp"
    elif language == "eng" or language == "":
        return "en"
    else:
        raise MetadataError(f"unexpected language {language} found.")

def process_degree(degree):
    degree = degree.strip()
    if degree == "Master of Architectural Stud":
        return "Master of Architectural Studies"
    elif degree == "Master of Information Tech":
        return "Master of Information Technology"
    elif degree == "":
        return FLAG
    return degree

def process_degree_abbreviation(degree, mappings):
    return mappings["abbreviation"].get(degree, FLAG)


def process_degree_discipline(discipline, mappings):
    discipline = discipline.strip()
    return mappings["discipline"].get(discipline, FLAG)


def process_degree_level(level):
    level = level.strip()
    if not level:
        raise MetadataError("degree level is missing")
    if level == "0":
        raise MetadataError("received undergraduate work, degree level is 0")
    if level != "1" and level != "2":
        raise MetadataError("invalid degree level")
    return level

def create_package_data(
    package_metadata_xml, name, doi_ident, agreements, package_path, mappings
):
    """Extract the package data from the package XML."""

    root = package_metadata_xml.getroot()

    title = root.findtext("dc:title", default="", namespaces=NAMESPACES)
    title = title.strip()
    if title == "":
        raise MetadataError("title tag is missing")

    creator = root.findtext("dc:creator", default="", namespaces=NAMESPACES)
    creator = creator.strip()
    if creator == "":
        raise MetadataError("creator tag is missing")

    subject_elements = root.findall("dc:subject", namespaces=NAMESPACES)
    subjects = process_subjects(subject_elements, mappings)

    description = root.findtext(
        "dc:description", default="", namespaces=NAMESPACES
    )
    description = process_description(description)

    publisher = root.findtext(
        "dc:publisher", default="", namespaces=NAMESPACES
    )
    publisher = publisher.strip()
    if publisher == "":
        publisher = "Carleton University"

    contributor_elements = root.findall(
        "dc:contributor", namespaces=NAMESPACES
    )
    contributors = process_contributors(contributor_elements)

    date = root.findtext("dc:date", default="", namespaces=NAMESPACES)
    date, year = process_date(date)

    language = root.findtext("dc:language", default="", namespaces=NAMESPACES)
    language = process_language(language)

    degree = root.findtext(
        "etdms:degree/etdms:name", default="", namespaces=NAMESPACES
    )
    degree = process_degree(degree)

    abbreviation = process_degree_abbreviation(degree, mappings)

    rights_notes = root.findtext(
        "dc:rights_notes", default="", namespaces=NAMESPACES
    )

    rights_notes = rights_notes.replace(rights_notes, "")
    if rights_notes == "":
        rights_notes = (
            f"Copyright © {year} the author(s). Theses may be used for "
            "non-commercial research, educational, or related academic "
            "purposes only. Such uses include personal study, distribution to "
            "students, research and scholarship. Theses may only be shared by "
            "linking to the Carleton University Institutional Repository and "
            "no part may be copied without proper attribution to the author; "
            "no part may be used for commercial purposes directly or "
            "indirectly via a for-profit platform; no adaptation or "
            "derivative works are permitted without consent from the "
            "copyright owner."
        )

    discipline = root.findtext(
        "etdms:degree/etdms:discipline", default="", namespaces=NAMESPACES
    )
    discipline = process_degree_discipline(discipline, mappings)

    level = root.findtext(
        "etdms:degree/etdms:level", default="", namespaces=NAMESPACES
    )
    level = process_degree_level(level)

    doi = f"{DOI_PREFIX}/etd/{year}-{doi_ident}"

    return PackageData(
        package_files=[],        
        creator=creator,
        contributors=contributors,
        date=year,
        type="thesis",
        description=description,
        publisher=publisher,
        doi=doi,
        language=language,
        rights_notes=rights_notes,
        title=title,
        subjects=subjects,
        agreements=agreements,
        degree=degree,
        degree_discipline=discipline,
        degree_level=level,
    )

def process_value(value):
    
    if isinstance(value, list):
        return '||'.join(map(str, value))
    elif isinstance(value, str):
        return value.strip()
    else:
        return str(value)

def add_to_csv(metadata_csv_path, package_data):
    
    package_files_list = package_data.package_files

    if isinstance(package_files_list, (list, tuple)):
        processed_files = []
        for filename in package_files_list:
            processed_files.append(str(filename).strip())
        package_files_str = '||'.join(processed_files)
    elif isinstance(package_files_list, str):
        package_files_str = package_files_list.strip()
    else:
        package_files_str = str(package_files_list).strip()

    row = [
        package_files_str,
        process_value(package_data.creator),
        process_value(package_data.contributors),
        process_value(package_data.date),
        process_value(package_data.type),
        process_value(package_data.description),
        process_value(package_data.publisher),
        process_value(package_data.doi),
        process_value(package_data.language),
        process_value(package_data.rights_notes),
        process_value(package_data.title),
        process_value(package_data.agreements),
        #TODO: come back later as it will need special handling
        #process_value(package_data.subjects)
    ]

    with open(
        metadata_csv_path, "a", newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_writer = csv.writer(metadata_csv_file, delimiter=',')
        csv_writer.writerow(row)
        
def copy_thesis_pdf(package_data, package_path, files_path):
    # ASSUMPTION: The file main thesis will always be a .pdf file.
    file_paths_in_data = glob.glob(os.path.join(package_path, "data", "*pdf"))

    largest_file_size = 0
    thesis_file_path = None

    # Because the files names are not consistent, get
    # the largest file ending in .pdf. Not foolproof.
    for potential_file_path in file_paths_in_data:
        size = os.path.getsize(potential_file_path)
        if size > largest_file_size:
            thesis_file_path = potential_file_path
            largest_file_size = size

    if not thesis_file_path:
        raise MetadataError("could not find pdf file")

    # We want an short pdf file name.
    # The first part is the creator name, simplified.
    dest_file_name = (
        package_data.creator.lower().replace(" ", "-").replace(",", "-")
    )

    # Add the double hyphen delimiter.
    dest_file_name += "--"

    # The second part is the title.
    # Adds new words to the filename from the title, but stop after 40
    # characters.
    ascii_letters_digits = string.ascii_letters + string.digits
    title_words = []
    title_words_len = 0
    for title_word in package_data.title.split():
        title_word_filtered = "".join(
            filter(lambda x: x in ascii_letters_digits, title_word)
        )
        if len(dest_file_name) + title_words_len > 120:
            break
        else:
            title_words.append(title_word_filtered)
            title_words_len += len(title_word_filtered)

    dest_file_name += "-".join(title_words)
    dest_file_name = dest_file_name.lower()
    dest_file_name += ".pdf"
    dest_path = os.path.join(files_path, dest_file_name)

    shutil.copy2(thesis_file_path, dest_path)
    return dest_file_name


def copy_package_files(package_data, package_path, files_path):
    thesis_file_name = copy_thesis_pdf(package_data, package_path, files_path)
    supplemental_path = os.path.join(package_path, "data", "supplemental")
    if os.path.isdir(supplemental_path):
        archive_file_name = f"{thesis_file_name[:-4]}-supplemental.zip"
        archive_path = os.path.join(files_path, archive_file_name)
        shutil.make_archive(archive_path[:-4], "zip", supplemental_path)
        return thesis_file_name, archive_file_name
    return (thesis_file_name,)

def create_local_metadata_xml(package_data, output_package_dir):
    root = ElementTree.Element("dublin_core", schema="thesis")

    if package_data.agreement:
        ElementTree.SubElement(root, "dcvalue", element="agreement", qualifier="name").text = package_data.agreement

    xml_string = ElementTree.tostring(root, encoding="unicode")
    xml_file_path = os.path.join(output_package_dir, "metadata_local.xml")

    try:
        with open(xml_file_path, "w", encoding="utf-8") as f:
            f.write(xml_string)
        print(f"Created metadata_local.xml in: {xml_file_path}")
    except Exception as e:
        print(f"Error writing metadata_local.xml: {e}")

def create_thesis_metadata_xml(package_data, output_package_dir):
    
    root = ElementTree.Element("dublin_core", schema="thesis")

    if package_data.degree:
        ElementTree.SubElement(root, "dcvalue", element="degree", qualifier="name").text = package_data.degree
    if package_data.degree_level:
        ElementTree.SubElement(root, "dcvalue", element="degree", qualifier="level").text = package_data.degree_level
    if package_data.degree_discipline:
        ElementTree.SubElement(root, "dcvalue", element="degree", qualifier="discipline").text = package_data.degree_discipline

    xml_string = ElementTree.tostring(root, encoding="unicode")
    xml_file_path = os.path.join(output_package_dir, "metadata_thesis.xml")

    try:
        with open(xml_file_path, "w", encoding="utf-8") as f:
            f.write(xml_string)
        print(f"Created metadata_thesis.xml in: {xml_file_path}")
    except Exception as e:
        print(f"Error writing metadata_thesis.xml: {e}")


def create_dspace_import(packages, metadata_csv_path, invalid_ok, doi_start, mappings, dspace_saf_path):

    output_path = "/home/manfred/etddepositor/dspace-csv-archive/output/"
    
    # A list of packages which failed during processing.
    failure_log: List[str] = []

    # Start the doi_ident counter at the provided doi_start number.
    doi_ident = doi_start
    
    click.echo(f"Processing {len(packages)} packages to create Hyrax import.")
    for index, package_path in enumerate(packages):
        name = os.path.basename(package_path)
        
        click.echo(f"{name}: ", nl=False)

        # Is the BagIt container valid? This will catch bit-rot errors early.
        if not bagit.Bag(package_path).is_valid() and not invalid_ok:
            err_msg = "Invalid BagIt."
            click.echo(err_msg)
            #failure_log.append(f"{name}: {err_msg}")
            continue
        try:
            
            permissions_path = os.path.join(
                package_path,
                "data",
                "meta",
                f"{name}_permissions_meta.txt",
            )
            with open(
                permissions_path, "r", encoding="utf-8"
            ) as permissions_file:
                permissions_file_content = permissions_file.readlines()

            #TODO: come back later and re introduce this
            #agreements = process_embargo_and_agreements(
            #    permissions_file_content, mappings
            #)
            
            item_id = index + 1
            item_dir_name = f"item_{item_id:03d}"
            item_output_dir = os.path.join(output_path, item_dir_name)
            os.makedirs(item_output_dir, mode=0o770, exist_ok=True)

            # Maybe re use this for the non complete?
            #dspace_path = os.path.join(dspace_saf_path, name)
            #os.makedirs(dspace_path, mode=0o770, exist_ok=True)

            package_metadata_xml_path = os.path.join(
                package_path, "data", "meta", f"{name}_etdms_meta.xml"
            )

            package_metadata_xml = ElementTree.parse(package_metadata_xml_path)
            agreements = True

            package_data = create_package_data(package_metadata_xml, name, doi_ident, agreements, package_path, mappings)
            package_data.package_files = copy_package_files(
                package_data, package_path, dspace_saf_path
            )

            add_to_csv(metadata_csv_path, package_data)
            create_thesis_metadata_xml(package_data, item_output_dir)
            
        except ElementTree.ParseError as e:
            err_msg = f"Error parsing XML, {e}."
            click.echo(err_msg)
            
        except MissingFileError as e:
            err_msg = f"Required file is missing, {e}."
            click.echo(err_msg)
        except MetadataError as e:
            err_msg = f"Metadata error, {e}."
            click.echo(err_msg)
        else:
            doi_ident += 1
            print('we go there')
            #hyrax_import_packages.append(package_data)
            click.echo("Done")

@click.command()
@click.argument('base_directory')
@click.option('--mapping_file', default='etddepositor/mappings.yaml', help='Path to the mappings YAML file.')
@click.option('--invalid_ok', is_flag=True, help='Continue processing even if BagIt is invalid.')
@click.option(
    "--doi-start",
    type=int,
    default=1,
    required=True,
    help="The starting number of the incrementing part of the generated DOIs.",
)
def process(base_directory, doi_start, invalid_ok, mapping_file):
    
    click.echo("Starting ETD processing...")

    processing_directory = base_directory

    # Load mappings
    mappings = load_mappings(mapping_file)
    if not mappings:
        return

    # Validate subject mappings
    validate_subject_mappings(mappings)
    done_path, marc_path, crossref_path, csv_report_path, dspace_saf_output_path = create_output_directories(processing_directory)

    # Find packages in the ready directory
    packages = find_etd_packages(processing_directory)
    click.echo(f"Found {len(packages)} packages to process.")

    if not packages:
        click.echo("No packages found. Exiting.")
        #TODO remove this eventually
        #return
    
    #click.echo(f"Outputting DSpace SAF to: {dspace_saf_output_path}")

    #TODO: YOU ARE STEALING THIS FROM CSV_REPORT YOU'LL NEED TO MAKE ANOTHER
    metadata_csv_path = os.path.join(dspace_saf_output_path, "metadata.csv")
    write_metadata_csv_header(metadata_csv_path)

    create_dspace_import(packages, metadata_csv_path, invalid_ok, doi_start, mappings, dspace_saf_output_path)
 

    click.echo("ETD processing complete.")

if __name__ == '__main__':
    process()
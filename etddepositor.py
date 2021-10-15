import collections
import csv
import datetime
import glob
import hashlib
import os
import shutil
import smtplib
import string
import subprocess
import time
import xml.etree.ElementTree as ElementTree
from typing import List

import bagit
import click
import pymarc
import requests
import yaml

# CONTEXT_SETTINGS is a click-specific config dict which allows us to define a
# prefix for the automatic environment variable option feature.
CONTEXT_SETTINGS = {"auto_envvar_prefix": "ETD_DEPOSITOR"}

# READY_SUBDIR is the name of the subdirectory under the provided processing
# directory where ETD packages are moved before they are processed.
READY_SUBDIR = "ready"

# DONE_SUBDIR is the name of the subdirectory under the provided processing
# directory where ETD packages are moved after they have been processed.
DONE_SUBDIR = "done"

# HYRAX_SUBDIR is the name of the subdirectory under the provided processing
# directory where the Hyrax import CSVs and files are created.
HYRAX_SUBDIR = "hyrax"

# FILES_SUBDIR is the name of the subdirectory under the Hyrax subdirectory
# where files are stored for Hyrax import.
FILES_SUBDIR = "files"

# MARC_SUBDIR is the name of the subdirectory under the provided processing
# directory where the MARC records for ETDs are created.
MARC_SUBDIR = "marc"

# CROSSREF_SUBDIR is the name of the subdirectory under the provided processing
# directory where the Crossref-ready metadata for ETDs are created.
CROSSREF_SUBDIR = "crossref"

# DOI_PREFIX is Carleton University Library's DOI prefix, used when minting new
# DOIs for ETDs.
DOI_PREFIX = "10.22215"

# DOI_URL_PREFIX is the prefix to add to DOIs to make them resolvable.
DOI_URL_PREFIX = "https://doi.org/"

# NAMESPACES is a dictionary of namespace prefixes to URIs, used when
# processing the FGPA provided metadata, which is in XML format.
NAMESPACES = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "etdms": "http://www.ndltd.org/standards/metadata/etdms/1.1/",
}

# PackageData is a container for package data, used to create the Hyrax
# import, MARC record, and Crossref records for an ETD.
PackageData = collections.namedtuple(
    "PackageData",
    [
        "name",
        "source_identifier",
        "title",
        "creator",
        "subjects",
        "abstract",
        "publisher",
        "contributors",
        "date",
        "language",
        "degree",
        "abbreviation",
        "discipline",
        "level",
        "url",
        "doi",
        "path",
    ],
)


class MissingFileError(Exception):
    """Raised when a required file is missing."""


class MetadataError(Exception):
    """Raised when a problem with the package metadata is encountered."""


class GetURLFailedError(Exception):
    """Raised when the Hyrax URL for an imported package can't be found."""


@click.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.option(
    "--processing-directory",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
    help=(
        "The directory under which the tool will store ETD packages, "
        "MARC records, and Crossref-ready metadata."
    ),
)
def etddepositor(ctx, processing_directory):
    """
    Carleton University Library - ETD Deposit Tool
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj["processing_directory"] = processing_directory


@etddepositor.command()
@click.pass_context
@click.option(
    "--inbox",
    "inbox_directory_path",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
    help="The directory containing the ITS copies of the thesis packages.",
)
def copy(ctx, inbox_directory_path):
    """
    Copy and extract ETD packages.

    Packages in the inbox directory will be moved and unpacked into the
    processing directory.
    Packages which are already in the processing directory will be ignored.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the package storage directories.
    ready_path = os.path.join(processing_directory, READY_SUBDIR)
    done_path = os.path.join(processing_directory, DONE_SUBDIR)

    # Does the ready subdirectory exist?
    if not os.path.isdir(ready_path):
        os.mkdir(ready_path, mode=0o770)

    # Find the packages that are already in the processing directory.
    existing_packages = [
        os.path.basename(x) for x in glob.glob(os.path.join(ready_path, "*"))
    ]
    existing_packages.extend(
        [
            os.path.basename(x)
            for x in glob.glob(os.path.join(done_path, "*", "*"))
        ]
    )

    # Find the zip archives in the inbox directory,
    # filtering out the already processed packages.
    new_package_paths = [
        path
        for path in glob.glob(os.path.join(inbox_directory_path, "*.zip"))
        if os.path.splitext(os.path.basename(path))[0] not in existing_packages
    ]

    # Extract the files from the inbox to the ready subdirectory.
    click.echo(
        f"Moving and unpacking {len(new_package_paths)} packages.",
    )
    for path in new_package_paths:
        click.echo(f"{os.path.basename(path)}: ", nl=False)
        try:
            shutil.unpack_archive(path, ready_path)
        except Exception as e:
            click.echo(f"Unable to extract {path}, {e}")
        else:
            click.echo("Done")


@etddepositor.command()
@click.pass_context
@click.option(
    "--importer",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.option(
    "--invalid-ok/--invalid-not-ok",
    default=False,
    help=(
        "Process packages that are not valid BagIt containers. "
        "This can be used to process packages which needed manual fixes."
    ),
)
@click.option(
    "--user-id",
    required=True,
    help="Passed as the user_id option when running the Hyrax importer.",
)
@click.option(
    "--auth-token",
    required=True,
    help="Passed as the auth_token option when running the Hyrax importer.",
)
@click.option(
    "--doi-start",
    type=int,
    default=1,
    required=True,
    help="The starting number of the incrementing part of the generated DOIs.",
)
@click.option(
    "--hyrax-host",
    required=True,
    help=(
        "The scheme and domain name of the Hyrax instance we are importing "
        "into. Passed as the url option when running the Hyrax importer and "
        "used to find the Hyrax URLs for imported works."
    ),
)
@click.option(
    "--smtp-host",
    required=True,
    help="The SMTP server to use when sending the email report.",
)
@click.option("--smtp-port", type=int, default=25, required=True)
@click.option(
    "--email-from",
    required=True,
    help="The 'from' address for the report email.",
)
@click.option(
    "--email-to", required=True, help="The 'to' address for the report email."
)
def process(
    ctx,
    importer,
    invalid_ok,
    user_id,
    auth_token,
    doi_start,
    hyrax_host,
    smtp_host,
    smtp_port,
    email_from,
    email_to,
):
    """
    Process all packages which are awaiting work.

    Add the metadata and files to the Hyrax import, run the import,
    create MARC and Crossref ready metadata files, and send the email report.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Load the mappings file.
    with open("mappings.yaml") as mappings_file:
        mappings = yaml.load(mappings_file, Loader=yaml.FullLoader)

    # Ensure the subjects in the mappings file are properly formatted.
    for code, subject in mappings["lc_subject"].items():
        for subject_tags in subject:
            if len(subject_tags) not in [2, 4]:
                click.echo(f"The subject {code} is not formatted correctly.")
                ctx.exit(1)

    # Get a list of the package directories in the ready subdirectory.
    ready_path = os.path.join(processing_directory, READY_SUBDIR)
    packages = glob.glob(os.path.join(ready_path, "*"))

    # If there are no packages, exit early.
    if not packages:
        click.echo(f"No packages in {READY_SUBDIR} to process.")
        ctx.exit()

    # Build the timestamp, used to name subdirectories under the done, Hyrax,
    # MARC, and Crossref subdirectories to compartmentalize each import.
    ts = f"{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}"

    # Build the processing location subdirectories.
    done_path = os.path.join(processing_directory, DONE_SUBDIR, ts)
    hyrax_path = os.path.join(processing_directory, HYRAX_SUBDIR, ts)
    files_path = os.path.join(hyrax_path, FILES_SUBDIR)
    marc_path = os.path.join(processing_directory, MARC_SUBDIR, ts)
    crossref_path = os.path.join(processing_directory, CROSSREF_SUBDIR, ts)

    # Create the subdirectories if they don't exist.
    os.makedirs(done_path, mode=0o770, exist_ok=True)
    for path in [files_path, marc_path, crossref_path]:
        os.makedirs(path, mode=0o775, exist_ok=True)

    # Create the import metadata.csv file and add the header.
    metadata_csv_path = os.path.join(hyrax_path, "metadata.csv")
    write_metadata_csv_header(metadata_csv_path)

    hyrax_import_packages, pre_import_failure_log = create_hyrax_import(
        packages,
        metadata_csv_path,
        files_path,
        invalid_ok,
        doi_start,
        mappings,
    )

    click.echo("Running Bulkrax importer.")
    subprocess.run(
        [
            importer,
            "--name",
            f"ETD-Deposit-{ts}",
            "--parser_klass",
            "Bulkrax::CsvParser",
            "--commit",
            "Create and Import",
            "--import_file_path",
            metadata_csv_path,
            "--override_rights_statement",
            "1",
            "--rights_statement",
            "http://rightsstatements.org/vocab/InC/1.0/",
            "--user_id",
            user_id,
            "--auth_token",
            auth_token,
            "--url",
            hyrax_host,
        ]
    )

    (
        completed_packages,
        crossref_et,
        post_import_failure_log,
    ) = post_import_processing(hyrax_import_packages, hyrax_host, marc_path)

    click.echo("Writing complete Crossref file: ", nl=False)
    crossref_file_path = os.path.join(
        crossref_path, f"{ datetime.date.today().isoformat()}-crossref.xml"
    )
    crossref_et.write(
        crossref_file_path, encoding="utf-8", xml_declaration=True
    )
    click.echo("Done")

    click.echo("Creating MARC archive: ", nl=False)
    marc_archive_path = os.path.join(
        processing_directory,
        MARC_SUBDIR,
        f"{ datetime.date.today().isoformat()}-marc-archive.zip",
    )
    # make_archive doesn't want the archive extension.
    shutil.make_archive(marc_archive_path[:-4], "zip", marc_path)
    click.echo("Done")

    click.echo("Sending report email: ", nl=False)
    send_email_report(
        completed_packages,
        pre_import_failure_log + post_import_failure_log,
        marc_archive_path,
        crossref_file_path,
        smtp_host,
        smtp_port,
        email_from,
        email_to,
    )
    click.echo("Done")

    click.echo("Moving processed packages to done subdirectory: ", nl=False)
    for package in completed_packages:
        shutil.move(package.path, done_path)
    click.echo("Done")


def write_metadata_csv_header(metadata_csv_path):
    """Write the header columns to the Hyrax import metadata CSV file."""

    header_columns = [
        "source_identifier",
        "model",
        "title",
        "creator",
        "identifier",
        "subject",
        "abstract",
        "publisher",
        "contributor",
        "date_created",
        "language",
        "degree",
        "degree_discipline",
        "degree_level",
        "resource_type",
        "file",
    ]

    with open(
        metadata_csv_path, "w", newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_writer = csv.writer(metadata_csv_file)
        csv_writer.writerow(header_columns)


def create_hyrax_import(
    packages, metadata_csv_path, files_path, invalid_ok, doi_start, mappings
):
    """Process each package to create the Hyrax import."""

    # Package data for packages which have been added to Hyrax import.
    hyrax_import_packages = []

    # A list of packages which failed during processing.
    failure_log: List[str] = []

    # Start the doi_ident counter at the provided doi_start number.
    doi_ident = doi_start

    click.echo(f"Processing {len(packages)} packages to create Hyrax import.")
    for package_path in packages:
        name = os.path.basename(package_path)
        click.echo(f"{name}: ", nl=False)

        # Is the BagIt container valid? This will catch bit-rot errors early.
        if not bagit.Bag(package_path).is_valid() and not invalid_ok:
            err_msg = "Invalid BagIt."
            click.echo(err_msg)
            failure_log.append(f"{name}: {err_msg}")
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
            # We pass a list of lines here instead of a file handle to
            # make unit testing easier.
            check_embargo_and_agreements(permissions_file_content)

            package_metadata_xml_path = os.path.join(
                package_path, "data", "meta", f"{name}_etdms_meta.xml"
            )
            package_metadata_xml = ElementTree.parse(package_metadata_xml_path)
            package_data = create_package_data(
                package_metadata_xml, name, doi_ident, package_path, mappings
            )
            package_files = copy_package_files(
                package_data, package_path, files_path
            )
            add_to_csv(metadata_csv_path, package_data, package_files)
        except ElementTree.ParseError as e:
            err_msg = f"Error parsing XML, {e}."
            click.echo(err_msg)
            failure_log.append(f"{name}: {err_msg}")
        except MissingFileError as e:
            err_msg = f"Required file is missing, {e}."
            click.echo(err_msg)
            failure_log.append(f"{name}: {err_msg}")
        except MetadataError as e:
            err_msg = f"Metadata error, {e}."
            click.echo(err_msg)
            failure_log.append(f"{name}: {err_msg}")
        else:
            doi_ident += 1
            hyrax_import_packages.append(package_data)
            click.echo("Done")

    return hyrax_import_packages, failure_log


def check_embargo_and_agreements(content_lines):
    """Check that we can process this ETD package.

    The package's permissions metadata must state that the embargo period has
    passed and that the student has signed the required agreements.
    """

    for line in content_lines:
        line = line.strip()
        if line.startswith(
            ("Student ID", "Thesis ID", "LAC Non-Exclusive License")
        ):
            continue
        elif line.startswith("Embargo Expiry"):
            current_date = datetime.date.today()
            expiry_date = line.split(" ")[2]
            embargo_date = embargo_string_to_datetime(expiry_date)
            print(embargo_date)
            if current_date < embargo_date:
                raise MetadataError(
                    f"the embargo date of {embargo_date} has not passed"
                )
            continue
        elif line.startswith(
            (
                "Academic Integrity Statement",
                "FIPPA",
                "Carleton University Thesis License Agreement",
            )
        ):
            if line.split("||")[2] != "Y":
                raise MetadataError(f"{line} is invalid")
            continue

        raise MetadataError(
            f"{line} was not expected in the permissions document content"
        )


def embargo_string_to_datetime(embargo):
    """Return the date representation of the embargo."""

    month_to_int = {
        "JAN": "1",
        "FEB": "2",
        "MAR": "3",
        "APR": "4",
        "MAY": "5",
        "JUN": "6",
        "JUL": "7",
        "AUG": "8",
        "SEP": "9",
        "OCT": "10",
        "NOV": "11",
        "DEC": "12",
    }
    embargo_split = embargo.split("-")
    try:
        month_number = month_to_int[embargo_split[1]]
        formatted_date = (
            f"{embargo_split[0]}/{month_number}/20{embargo_split[2]}"
        )
        return datetime.datetime.strptime(formatted_date, "%d/%m/%Y").date()
    except (IndexError, ValueError):
        raise MetadataError(f"embargo date {embargo} could not be processed")


def create_package_data(
    package_metadata_xml, name, doi_ident, package_path, mappings
):
    """Extract the package data from the package XML."""

    source_identifier = hashlib.sha256(name.encode("utf-8")).hexdigest()

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
    description = process_description(description, mappings)

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
    date = process_date(date)

    language = root.findtext("dc:language", default="", namespaces=NAMESPACES)
    language = process_language(language)

    degree = root.findtext(
        "etdms:degree/etdms:name", default="", namespaces=NAMESPACES
    )
    degree = process_degree(degree)

    abbreviation = process_degree_abbreviation(degree, mappings)

    discipline = root.findtext(
        "etdms:degree/etdms:discipline", default="", namespaces=NAMESPACES
    )
    discipline = process_degree_discipline(discipline, mappings)

    level = root.findtext(
        "etdms:degree/etdms:level", default="", namespaces=NAMESPACES
    )
    level = process_degree_level(level)

    doi = f"{DOI_PREFIX}/etd/{date}-{doi_ident}"

    return PackageData(
        name=name,
        source_identifier=source_identifier,
        title=title,
        creator=creator,
        subjects=subjects,
        abstract=description,
        publisher=publisher,
        contributors=contributors,
        date=date,
        language=language,
        degree=degree,
        abbreviation=abbreviation,
        discipline=discipline,
        level=level,
        url="",
        doi=doi,
        path=package_path,
    )


def process_subjects(subject_elements, mappings):
    subjects = []
    for subject_element in subject_elements:
        subject_code = subject_element.text.strip()
        if subject_code in mappings["lc_subject"]:
            for subject_tags in mappings["lc_subject"][subject_code]:
                subjects.append(subject_tags)
    return subjects


def process_description(description, mappings):
    description = description.strip()
    for symbol, replacement in mappings["description_escape_table"].items():
        description = description.replace(symbol, replacement)
    return description


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
    """Ensure date is properly formatted, then return the year as a string"""

    date = date.strip()
    if not date:
        raise MetadataError("date tag is missing")
    try:
        return str(datetime.datetime.strptime(date, "%Y-%M-%d").year)
    except ValueError:
        raise MetadataError(f"date value {date} is not properly formatted")


def process_language(language):
    language = language.strip()
    if language == "fre" or language == "fra":
        return "fra"
    elif language == "eng" or language == "":
        return "eng"
    else:
        raise MetadataError(f"unexpected language {language} found.")


def process_degree(degree):
    degree = degree.strip()
    if degree == "Master of Architectural Stud":
        return "Master of Architectural Studies"
    elif degree == "Master of Information Tech":
        return "Master of Information Technology"
    elif degree == "":
        return "FLAG"
    return degree


def process_degree_abbreviation(degree, mappings):
    return mappings["abbreviation"].get(degree, "FLAG")


def process_degree_discipline(discipline, mappings):
    discipline = discipline.strip()
    return mappings["discipline"].get(discipline, "FLAG")


def process_degree_level(level):
    level = level.strip()
    if not level:
        raise MetadataError("degree level is missing")
    if level == "0":
        raise MetadataError("received undergraduate work, degree level is 0")
    if level != "1" and level != "2":
        raise MetadataError("invalid degree level")
    return level


def copy_package_files(package_data, package_path, files_path):
    thesis_file_name = copy_thesis_pdf(package_data, package_path, files_path)
    supplemental_path = os.path.join(package_path, "data", "supplemental")
    if os.path.isdir(supplemental_path):
        archive_file_name = f"{thesis_file_name[:-4]}-supplemental.zip"
        archive_path = os.path.join(files_path, archive_file_name)
        shutil.make_archive(archive_path[:-4], "zip", supplemental_path)
        return thesis_file_name, archive_file_name
    return (thesis_file_name,)


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
    for title_word in package_data.title.split():
        dest_file_name += "".join(
            filter(lambda x: x in ascii_letters_digits, title_word)
        )
        if len(dest_file_name) > 120:
            break
        else:
            dest_file_name += "-"

    dest_file_name = dest_file_name.lower()
    dest_file_name += ".pdf"
    dest_path = os.path.join(files_path, dest_file_name)

    shutil.copy2(thesis_file_path, dest_path)
    return dest_file_name


def add_to_csv(metadata_csv_path, package_data, package_files):
    """Writes the package metadata to the Hyrax import CSV."""

    row = [
        package_data.source_identifier,
        "Etd",
        package_data.title,
        package_data.creator,
        f"DOI: {DOI_URL_PREFIX}{package_data.doi}",
        create_csv_subject(package_data.subjects),
        package_data.abstract,
        package_data.publisher,
        "|".join(package_data.contributors),
        package_data.date,
        package_data.language,
        package_data.degree,
        package_data.discipline,
        package_data.level,
        "Thesis",
        "|".join(package_files),
    ]

    with open(
        metadata_csv_path, "a", newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_writer = csv.writer(metadata_csv_file)
        csv_writer.writerow(row)


def create_csv_subject(subjects):
    csv_subjects = []
    for subject_tags in subjects:
        a_text = subject_tags[1]
        a_text = a_text.replace(".", "")
        csv_subject = a_text
        if len(subject_tags) == 4:
            x_text = subject_tags[4]
            x_text = x_text.replace(".", "")
            csv_subject = f"{csv_subject} -- {x_text}"
        csv_subjects.append(csv_subject)
    return "|".join(csv_subjects)


def post_import_processing(hyrax_import_packages, hyrax_host, marc_path):

    # Package data for packages which have been successfully imported
    # into Hyrax.
    completed_packages = []

    # Create the ElementTree and body element which will be used to create the
    # Crossref XML.
    crossref_et, body_element = create_crossref_etree()

    # A list of packages which failed during processing.
    failure_log: List[str] = []

    click.echo(
        f"Post-import processing for {len(hyrax_import_packages)} packages."
    )
    for package_data in hyrax_import_packages:
        click.echo(f"{package_data.name}: ", nl=False)
        try:
            add_url(package_data, hyrax_host)
            create_marc_record(package_data, marc_path)
            body_element.append(create_dissertation_element(package_data))
        except GetURLFailedError:
            err_msg = "Link not found in Hyrax."
            click.echo(err_msg)
            failure_log.append(f"{package_data.name}: {err_msg}")
        except pymarc.exceptions.PymarcException as e:
            err_msg = f"MARC error {e}"
            click.echo(err_msg)
            failure_log.append(f"{package_data.name}: {err_msg}")
        else:
            completed_packages.append(package_data)
            click.echo("Done")

    return completed_packages, crossref_et, failure_log


def add_url(package_data, hyrax_host):
    for wait in range(10):
        time.sleep(wait * wait)
        resp = requests.get(
            hyrax_host
            + "/catalog.json?sourcetesim="
            + package_data.source_identifier
        )
        if resp.status_code == 200:
            json = resp.json()
            for doc in json["response"]["docs"]:
                if (
                    "source_tesim" in doc
                    and doc["source_tesim"][0]
                    == package_data.source_identifier
                ):
                    work_id = doc["id"]
                    return package_data._replace(
                        url=f"{hyrax_host}/concern/works/{work_id}"
                    )
    raise GetURLFailedError


def create_marc_record(package_data, marc_path):
    """
    Create a MARC encoded record for an ETD package
    """

    subtitle = ""

    if ":" in package_data.title:
        split_title = package_data.title.split(":", 1)
        processed_title = split_title[0].strip() + " :"
        subtitle = split_title[1].strip()
        if subtitle[-1] != ".":
            subtitle = subtitle + "."
    else:
        processed_title = package_data.title.strip()
        if processed_title[-1] != ".":
            processed_title = processed_title + "."

    title_field = pymarc.Field(
        tag="245",
        indicators=["1", "0"],
        subfields=[
            "a",
            processed_title,
        ],
    )

    if subtitle != "":
        title_field.add_subfield("b", subtitle)

    processed_author = package_data.creator.strip()
    if processed_author[-1] != "-":
        processed_author = processed_author + ","

    today = datetime.date.today()

    record = pymarc.Record(force_utf8=True, leader="     nam a22     4i 4500")
    record.add_field(
        pymarc.Field(
            tag="006",
            data="m     o  d        ",
        )
    )
    record.add_field(
        pymarc.Field(
            tag="007",
            data="cr || ||||||||",
        )
    )
    record.add_field(
        pymarc.Field(
            tag="008",
            data="{}s{}    onca||||omb|| 000|0 eng d".format(
                today.strftime("%y%m%d"), package_data.date
            ),
        )
    )
    record.add_field(
        pymarc.Field(
            tag="040",
            indicators=[" ", " "],
            subfields=[
                "a",
                "CaOOCC",
                "b",
                "eng",
                "e",
                "rda",
                "c",
                "CaOOCC",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="100",
            indicators=["1", " "],
            subfields=[
                "a",
                processed_author,
                "e",
                "author",
            ],
        )
    )
    record.add_field(title_field)
    record.add_field(
        pymarc.Field(
            tag="264",
            indicators=[" ", "1"],
            subfields=["a", "Ottawa,", "c", package_data.date],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="264",
            indicators=[" ", "4"],
            subfields=["c", "\u00A9" + package_data.date],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="300",
            indicators=[" ", " "],
            subfields=["a", "1 online resource :", "b", "illustrations"],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="336",
            indicators=[" ", " "],
            subfields=[
                "a",
                "text",
                "b",
                "txt",
                "2",
                "rdacontent",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="337",
            indicators=[" ", " "],
            subfields=[
                "a",
                "computer",
                "b",
                "c",
                "2",
                "rdamedia",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="338",
            indicators=[" ", " "],
            subfields=[
                "a",
                "online resource",
                "b",
                "cr",
                "2",
                "rdacarrier",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="502",
            indicators=[" ", " "],
            subfields=[
                "a",
                "Thesis ("
                + package_data.abbreviation
                + ") - Carleton University, "
                + package_data.date
                + ".",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="504",
            indicators=[" ", " "],
            subfields=["a", "Includes bibliographical references."],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="540",
            indicators=[" ", " "],
            subfields=[
                "a",
                (
                    "Licensed through author open access agreement. "
                    "Commercial use prohibited without author's consent."
                ),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="591",
            indicators=[" ", " "],
            subfields=["a", "e-thesis deposit", "9", "LOCAL"],
        )
    )
    for subject_tags in package_data.subjects:
        record.add_field(
            pymarc.Field(
                tag="650", indicators=[" ", "0"], subfields=subject_tags
            )
        )
    record.add_field(
        pymarc.Field(
            tag="710",
            indicators=["2", " "],
            subfields=[
                "a",
                "Carleton University.",
                "k",
                "Theses and Dissertations.",
                "g",
                package_data.discipline + ".",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="856",
            indicators=["4", "0"],
            subfields=[
                "u",
                package_data.url,
                "z",
                "Free Access (Digital Library Full Text)",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="979",
            indicators=[" ", " "],
            subfields=[
                "a",
                "MARC file generated {} on ETD Depositor".format(
                    today.isoformat()
                ),
                "9",
                "LOCAL",
            ],
        )
    )

    with open(
        os.path.join(marc_path, package_data.name + "_marc.mrc"), "wb"
    ) as marc_file:
        marc_file.write(record.as_marc())


def create_crossref_etree():
    doi_batch = ElementTree.Element(
        "doi_batch",
        attrib={
            "version": "4.4.1",
            "xmlns": "http://www.crossref.org/schema/4.4.1",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://www.crossref.org/schema/4.4.1 "
                "http://www.crossref.org/schemas/crossref4.4.1.xsd"
            ),
        },
    )

    head = ElementTree.SubElement(doi_batch, "head")
    doi_batch_id = ElementTree.SubElement(head, "doi_batch_id")
    doi_batch_id.text = str(int(time.time()))
    timestamp = ElementTree.SubElement(head, "timestamp")
    timestamp.text = f"{time.time()*1e7:.0f}"

    depositor = ElementTree.SubElement(head, "depositor")
    depositor_name = ElementTree.SubElement(depositor, "depositor_name")
    depositor_name.text = "Carleton University Library"
    email_address = ElementTree.SubElement(depositor, "email_address")
    email_address.text = "doi@library.carleton.ca"

    registrant = ElementTree.SubElement(head, "registrant")
    registrant.text = "Carleton University"
    body = ElementTree.SubElement(doi_batch, "body")

    tree = ElementTree.ElementTree(doi_batch)
    return tree, body


def create_dissertation_element(package_data):
    dissertation = ElementTree.Element("dissertation")

    person_name = ElementTree.SubElement(
        dissertation,
        "person_name",
        attrib={"contributor_role": "author", "sequence": "first"},
    )
    # Unfortunately, Crossref still expects first and last name.
    split_name = package_data.creator.split(",")
    surname_text = split_name[0].strip()
    if len(split_name) == 2:
        given_name_text = split_name[1].strip()
    else:
        given_name_text = ""
    given_name = ElementTree.SubElement(person_name, "given_name")
    given_name.text = given_name_text
    surname = ElementTree.SubElement(person_name, "surname")
    surname.text = surname_text

    titles = ElementTree.SubElement(dissertation, "titles")
    title = ElementTree.SubElement(titles, "title")
    title.text = package_data.title

    approval_date = ElementTree.SubElement(
        dissertation, "approval_date", attrib={"media_type": "online"}
    )
    year = ElementTree.SubElement(approval_date, "year")
    year.text = package_data.date

    institution = ElementTree.SubElement(dissertation, "institution")
    institution_name = ElementTree.SubElement(institution, "institution_name")
    institution_name.text = "Carleton University"
    institution_place = ElementTree.SubElement(
        institution, "institution_place"
    )
    institution_place.text = "Ottawa, Ontario"

    degree = ElementTree.SubElement(dissertation, "degree")
    degree.text = package_data.degree

    doi_data = ElementTree.SubElement(dissertation, "doi_data")
    doi = ElementTree.SubElement(doi_data, "doi")
    doi.text = package_data.doi
    resource = ElementTree.SubElement(doi_data, "resource")
    resource.text = package_data.url

    return dissertation


def send_email_report(
    completed_packages,
    failure_log,
    marc_archive_path,
    crossref_file_path,
    smtp_host,
    smtp_port,
    email_from,
    email_to,
):
    """Send the email report of completed and failed packages.

    This function also attaches the MARC archive and Crossref file."""

    from email.message import EmailMessage

    msg = EmailMessage()

    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = (
        f"ETD Depositor Report - {len(completed_packages)} processed, "
        f"{len(failure_log)} failed"
    )

    contents = (
        "ETD Depository Report - Run on "
        f"{datetime.date.today().isoformat()}.\n\n"
    )
    contents += f"{len(completed_packages)} completed packages.\n"
    for package_data in completed_packages:
        contents += (
            f"{package_data.name} {package_data.title} "
            f"{package_data.creator} {package_data.doi}\n"
        )
    contents += "\n"
    contents += f"{len(failure_log)} failed packages.\n"
    for line in failure_log:
        contents += f"{line}\n"

    msg.set_content(contents)

    with open(marc_archive_path, "rb") as marc_archive_file:
        marc_archive_data = marc_archive_file.read()
    msg.add_attachment(
        marc_archive_data,
        maintype="application",
        subtype="zip",
        filename=os.path.basename(marc_archive_path),
    )

    with open(crossref_file_path, "rb") as crossref_file:
        crossref_file_data = crossref_file.read()
    msg.add_attachment(
        crossref_file_data,
        maintype="application",
        subtype="xml",
        filename=os.path.basename(crossref_file_path),
    )

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.send_message(msg)
    server.quit()


if __name__ == "__main__":
    etddepositor(obj={})

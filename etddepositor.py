import bagit
import click
import collections
import csv
import datetime
import glob
import os
import os.path
import pymarc
import requests
import shutil
import smtplib
import subprocess
import time
import warnings
import xml.etree.ElementTree as ET
from xml.dom import minidom
import yaml
import zipfile

# AWAITING_WORK_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied to before they are processed.
AWAITING_WORK_SUBDIR = "awaiting_work"

# IN_PROGRESS_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied when they are being worked on.
IN_PROGRESS_SUBDIR = "in_progress"

# COMPLETE_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied after they have been processed.
COMPLETE_SUBDIR = "complete"

FILES_SUBDIR = "files"

# MARC_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are used to create marc records
MARC_SUBDIR = "marc"


CROSSREF_SUBDIR = "crossref"

# NAMESPACES is a dictionary of namespace prefixes to URIs.
NAMESPACES = {"dc": "http://purl.org/dc/elements/1.1/"}


# CONTEXT_SETTINGS is a click-specific config dict which allows us to define a
# prefix for the automatic environment variable option feature.
CONTEXT_SETTINGS = {"auto_envvar_prefix": "ETD_DEPOSITOR"}

ETDPackageData = collections.namedtuple(
    "ETDPackageData",
    [
        "source_identifier",
        "title",
        "creator",
        "pro_subject",
        "lc_subject",
        "description",
        "publisher",
        "contributor",
        "date_created",
        "language",
        "name",
        "discipline",
        "level",
        "resource_type",
    ],
)

CrossRefData = collections.namedtuple(
    "CrossRefData",
    [
        "given_name",
        "surname",
        "title",
        "approval_date",
        "degree",
        "identifier",
        "resource",
    ],
)

DOI_PREFIX = "10.22215"

log_success_array = []
log_failed_array = []


class InvalidBag(Exception):
    pass


class PermissionsInvalid(Exception):
    def __init__(self, var):
        self.var = var

    pass


class StillInEmbargo(PermissionsInvalid):
    pass


class RequiredAgreementNotSigned(PermissionsInvalid):
    pass


class UnexpectedLine(PermissionsInvalid):
    pass


class ProcessDataError(Exception):
    def __init__(self, message):
        self.message = message

    pass


class MarcError(Exception):
    pass


@click.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.option(
    "--processing-directory",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
def etddepositor(ctx, processing_directory):
    """
    Carleton University Library - ETD Deposit Processing Tool
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj["processing_directory"] = processing_directory


@etddepositor.command()
@click.pass_context
@click.option(
    "--inbox",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
def copy(ctx, inbox):
    """
    Copy and extract ETD packages from the ITS directory to our local directory.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the awaiting work location path.
    awaiting_work_path = os.path.join(processing_directory, AWAITING_WORK_SUBDIR)

    # Does that path exist?
    if not os.path.isdir(awaiting_work_path):
        click.echo(f"{awaiting_work_path} does not exist yet. Creating now...")
        os.mkdir(awaiting_work_path, mode=0o770)

    # Find the packages that are already in the processing location.
    existing_packages = [
        os.path.basename(x)
        for x in glob.glob(os.path.join(processing_directory, "*", "*"))
    ]

    # Find the list of bags in the ITS directory which aren't already processed.
    new_package_paths = [
        filepath
        for filepath in glob.glob(os.path.join(inbox, f"*.zip"))
        if os.path.splitext(os.path.basename(filepath))[0] not in existing_packages
    ]

    # Extract the files from the ITS directory to awaiting work
    for filepath in new_package_paths:
        click.echo(
            f"Moving bag and extracting {os.path.basename(filepath)}...", nl=False
        )
        try:
            with zipfile.ZipFile(filepath, "r") as packagezip:
                packagezip.extractall(awaiting_work_path)
        except zipfile.BadZipFile as e:
            click.echo(f"{filepath} is a bad zip: {e}")
        except Exception as e:
            click.echo(f"Unable to extract {filepath}: {e}")
        click.echo("Done")


@etddepositor.command()
@click.pass_context
@click.option(
    "--importer",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.option("--invalid-ok/--invalid-not-ok", default=False)
@click.option("--user_id", type=str, required=True)
@click.option("--auth_token", type=str, required=True)
@click.option("--identifier", type=int, default=-1, required=True)
@click.option("--target", type=str, required=True)
@click.option("--host", type=str, required=True)
@click.option("--port", type=int, required=True)
@click.option("--source", type=str, required=True)
@click.option("--destination", type=str, required=True)
def process(
    ctx,
    importer,
    user_id,
    auth_token,
    identifier,
    target,
    host,
    port,
    source,
    destination,
    invalid_ok=False,
):
    """
    Find the oldest unprocessed ETD package and process it.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the three processing location subdirectory paths.
    paths = []
    awaiting_work_path = os.path.join(processing_directory, AWAITING_WORK_SUBDIR)
    in_progress_path = os.path.join(processing_directory, IN_PROGRESS_SUBDIR)
    complete_path = os.path.join(processing_directory, COMPLETE_SUBDIR)
    files_path = os.path.join(in_progress_path, FILES_SUBDIR)
    marc_path = os.path.join(processing_directory, MARC_SUBDIR)
    crossref_path = os.path.join(processing_directory, CROSSREF_SUBDIR)

    paths.append(awaiting_work_path)
    paths.append(in_progress_path)
    paths.append(complete_path)
    paths.append(files_path)
    paths.append(marc_path)
    paths.append(crossref_path)

    # Do the 'in progress' and 'complete' directories exist?
    for path in paths:
        if not os.path.isdir(path):
            click.echo(f"{path} does not exist yet. Creating now...")
            os.mkdir(path, mode=0o770)

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)

    # Get a list of the package directories that are in the awaiting work directory.
    packages_awaiting_work = glob.glob(os.path.join(awaiting_work_path, "*"))

    packages = []

    # Checks if the awaiting work directory has packages to process
    if not os.listdir(awaiting_work_path):  # flake8: noqa: C901
        click.echo("No valid packages found to process.")
        raise click.Abort

    for package_path in packages_awaiting_work:
        click.echo(
            "--------------------------------------------------------------------------------"
        )

        if package_path and not invalid_ok and not bagit.Bag(package_path).is_valid():
            click.echo(
                f"Unable to process {os.path.basename(package_path)}, BagIt file is not valid."
            )
            log_failed_array.append(
                f"{os.path.basename(package_path)} | BagIt file is not valid"
            )
            continue
        click.echo(
            f"Moving {os.path.basename(package_path)} to 'in progress' directory."
        )

        shutil.move(package_path, in_progress_path)

        try:
            packages.append(
                process_data(
                    package_path,
                    processing_directory,
                    in_progress_path,
                    files_path,
                    config_yaml,
                )
            )

        except ProcessDataError as e:
            click.echo(f"Failed to process data for {os.path.basename(package_path)}")
            click.echo(e.message)
            log_failed_array.append(os.path.basename(package_path) + " | " + e.message)
        package_path = None

    metadata_path = in_progress_path + "/metadata.csv"
    click.echo(
        "--------------------------------------------------------------------------------"
    )
    subprocess.run(
        [
            importer,
            "--name",
            "CSV_Import",
            "--parser_klass",
            "Bulkrax::CsvParser",
            "--commit",
            "Create and Import",
            "--import_file_path",
            metadata_path,
            "--override_rights_statement",
            "1",
            "--rights_statement",
            "http://rightsstatements.org/vocab/InC/1.0/",
            "--user_id",
            user_id,
            "--auth_token",
            auth_token,
            "--url",
            target,
        ]
    )

    # Wait for imports to process
    click.echo("Getting work id...")
    time.sleep(20 * len(packages))

    work_link_dict = {}
    remove_packages = []

    # Loop and find the work id for each work that was imported
    for package in packages:
        work_id, remove_packages = get_work_id(target, package, remove_packages)
        if work_id != "":
            work_link_dict[package.source_identifier] = (
                target + "/concern/works/" + work_id
            )

    # Pythonic way of removing packages that were not successfully imported
    packages = [
        pack for pack in packages if pack.source_identifier not in remove_packages
    ]

    click.echo("\nCreating MARC record for packages...")
    doi_link = {}

    # Loop through successful works, create marc record and add the crossref entry for the work
    for package in packages:
        click.echo(
            "--------------------------------------------------------------------------------"
        )
        create_marc_record(
            package.source_identifier,
            marc_path,
            work_link_dict[package.source_identifier],
            package,
        )
        click.echo("MARC record successfully created")

        crossref_data = create_crossref_data(package, identifier, work_link_dict)

        # Naming of XML files for DOIs
        running_file = os.path.join(
            crossref_path, str(datetime.date.today()) + "-running.xml"
        )
        crossref_file = str(datetime.date.today()) + "-crossref.xml"
        crossref_file_path = os.path.join(crossref_path, crossref_file)

        # Create crossref entry, return doi link for created entry
        doi_link[package.source_identifier] = create_crossref(
            crossref_data, crossref_file_path, running_file
        )
        identifier += 1
        click.echo(f"crossref xml entry for {package.source_identifier} created!")

        # Log successful bagits
        log_message = (
            package.source_identifier
            + " | "
            + work_link_dict[package.source_identifier]
            + " | "
            + doi_link[package.source_identifier]
            + "\nProcessed on: "
            + str(datetime.date.today())
        )
        log_success_array.append(log_message)

        # Move bagit to completed directory
        click.echo(
            f"Moving new bagit {package.source_identifier} to 'complete' directory."
        )

        shutil.move(in_progress_path + "/" + package.source_identifier, complete_path)

    updated_metadata, importer_id = update_metadata(
        metadata_path, doi_link, in_progress_path, target
    )

    click.echo("Updated csv metadata")
    # Import the bagit package to hyrax
    click.echo(
        "--------------------------------------------------------------------------------"
    )

    click.echo("Re-Importing metadata...")

    subprocess.run(
        [
            importer,
            "--importer_id",
            str(importer_id),
            "--parser_klass",
            "Bulkrax::CsvParser",
            "--commit",
            "Update and Re-Import (update metadata only)",
            "--import_file_path",
            updated_metadata,
            "--override_rights_statement",
            "1",
            "--rights_statement",
            "http://rightsstatements.org/vocab/InC/1.0/",
            "--user_id",
            user_id,
            "--auth_token",
            auth_token,
            "--url",
            target,
        ]
    )

    email_report(marc_path, host, port, source, destination)


def process_data(
    oldest_etd_package_path,
    processing_directory,
    in_progress_path,
    files_path,
    config_yaml,
):
    """
    Process the individual package, extracting metadata for upload
    """
    in_progress_package_path = os.path.join(
        in_progress_path, os.path.basename(oldest_etd_package_path)
    )

    package_basename = os.path.basename(in_progress_package_path)
    permissions_metadata_path = os.path.join(
        in_progress_package_path,
        "data",
        "meta",
        f"{package_basename}_permissions_meta.txt",
    )

    with open(permissions_metadata_path, "r") as permissions_document:
        validate_permissions_document(permissions_document.read())

    package_metadata_xml_path = os.path.join(
        in_progress_package_path, "data", "meta", f"{package_basename}_etdms_meta.xml"
    )

    # Obtain a tuple of data corresponding to the metadata XML
    tree = ET.parse(package_metadata_xml_path)
    package_data = extract_metadata(tree.getroot(), package_basename, config_yaml)

    # Remove uneeded files and directories from the data directory
    """
    shutil.rmtree(os.path.join(in_progress_package_path + "/data", "meta"))
    shutil.rmtree(os.path.join(in_progress_package_path + "/data", "LAC"))
    if os.path.isdir(os.path.join(in_progress_package_path + "/data", "contributor")):
        shutil.rmtree(os.path.join(in_progress_package_path + "/data", "contributor"))
    """
    click.echo("Validation and metadata processing complete")
    csv_exporter(package_data, in_progress_path, in_progress_package_path, files_path)
    click.echo("Package process complete!")

    return package_data


def validate_permissions_document(content):

    for line in content.strip().split("\n"):
        if line.startswith("Student ID"):
            continue
        elif line.startswith("Thesis ID"):
            continue
        elif line.startswith("Embargo Expiry"):
            current_date = datetime.date.today()
            expiry_date = line.split(" ")[2]
            embargo_date = embargo_string_to_datetime(expiry_date)
            if current_date < embargo_date:
                raise ProcessDataError(
                    f"The embargo date of {embargo_date} has not passed"
                )
        elif line.startswith("LAC Non-Exclusive License"):
            continue
        elif line.startswith(
            (
                "Academic Integrity Statement",
                "FIPPA",
                "Carleton University Thesis License Agreement",
            )
        ):
            if line.split("||")[2] != "Y":
                raise ProcessDataError(f"{line} is invalid")
        else:
            raise ProcessDataError(
                f"{line} was not expected in the permissions document content"
            )


def embargo_string_to_datetime(embargo):

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
    month_number = month_to_int[embargo_split[1]]
    formatted_date = f"{embargo_split[0]}/{month_number}/{embargo_split[2]}"
    return datetime.datetime.strptime(formatted_date, "%d/%m/%y").date()


def extract_metadata(root, package_basename, config_yaml):

    title = root.findall("dc:title", namespaces=NAMESPACES)
    creator = root.findall("dc:creator", namespaces=NAMESPACES)

    if not title:
        raise ProcessDataError("title tag is missing")
    else:
        if title[0].text.strip() == "":
            raise ProcessDataError("title tag is missing")
    if not creator:
        raise ProcessDataError("creator tag is missing")
    else:
        if creator[0].text.strip() == "":
            raise ProcessDataError("creator tag is missing")

    title = title[0].text.strip()
    creator = creator[0].text.strip()

    subject = root.findall("dc:subject", namespaces=NAMESPACES)
    description = root.findall("dc:description", namespaces=NAMESPACES)
    publisher = root.findall("dc:publisher", namespaces=NAMESPACES)
    contributor = root.findall("dc:contributor", namespaces=NAMESPACES)
    date = root.findall("dc:date", namespaces=NAMESPACES)
    language = root.findall("dc:language", namespaces=NAMESPACES)

    pro_subject = check_pro_subject(subject, config_yaml)
    lc_subject = check_lc_subject(subject, config_yaml)
    description = check_description(description, config_yaml)
    publisher = publisher[0].text.strip()

    if contributor:
        contributor = check_contributor(contributor)
    else:
        contributor = ""

    date = check_date(date)
    date_created = date[:4]
    language = check_language(language)

    name = root.findall(".//{http://www.ndltd.org/standards/metadata/etdms/1.1/}name")
    discipline = root.findall(
        ".//{http://www.ndltd.org/standards/metadata/etdms/1.1/}discipline"
    )
    level = root.findall(".//{http://www.ndltd.org/standards/metadata/etdms/1.1/}level")

    if not name:
        raise ProcessDataError("name tag is missing")
    if not discipline:
        raise ProcessDataError("discipline tag is missing")
    if not level:
        raise ProcessDataError("level tag is missing")

    name = check_degree_name(name)
    discipline = check_degree_discipline(discipline, config_yaml)
    level, resource_type = check_degree_level(level)

    data = ETDPackageData(
        source_identifier=package_basename,
        title=title,
        creator=creator,
        pro_subject=pro_subject,
        lc_subject=lc_subject,
        description=description,
        publisher=publisher,
        contributor=contributor,
        date_created=date_created,
        language=language,
        name=name,
        discipline=discipline,
        level=level,
        resource_type=resource_type,
    )

    return data


def check_pro_subject(data, config_yaml):
    pro_subject = ""
    for i in range(len(data)):
        pro_subject = pro_subject + config_yaml["proquest_subject"].get(
            data[i].text, ""
        )
        if i < (len(data) - 1):
            pro_subject = pro_subject + " | "
    return pro_subject


def check_lc_subject(data, config_yaml):
    lc_subject = []
    for i in range(len(data)):
        lc_subject.append(config_yaml["lc_subject"].get(data[i].text, [["a", ""]]))
    return lc_subject


def check_description(data, config_yaml):
    data = data[0].text.strip()
    data = data.replace("\n", " ")
    data = data.replace("\r", "")
    data = data.replace("\u2018", "'")
    data = data.replace("\u2019", "'")
    data = data.replace("\u201c", '"')
    data = data.replace("\u201d", '"')
    data = data.replace("\u2013", "-")
    for symbol in config_yaml["html_escape_table"].keys():
        data = data.replace(config_yaml["html_escape_table"][symbol], symbol)
    return data


def check_contributor(data):
    contributor_string = ""
    for i in range(len(data)):
        contributor_string = contributor_string + data[i].text
        if i < (len(data) - 1):
            contributor_string = contributor_string + " | "
    return contributor_string


def check_language(data):
    if data[0].text.strip() != "eng" and "fre":
        warnings.warn("language tag was not an expected eng or fre tag")
    if data[0].text.strip() == "eng":
        return "English"
    elif data[0].text.strip() == "fre":
        return "French"


def check_date(data):
    try:
        datetime.datetime.strptime(data[0].text.strip(), "%Y-%M-%d")
    except Exception as e:
        print(e)
        pass
    return data[0].text.strip()


def check_degree_name(data):
    if data[0].text.strip() == "Master of Architectural Stud":
        return "Master of Architectural Studies"
    elif data[0].text.strip() == "Master of Information Tech":
        return "Master of Information Technology"
    elif data[0].text.strip() == "":
        return "FLAG"
    return data[0].text.strip()


def check_degree_discipline(data, config_yaml):
    return config_yaml["degree_discipline"].get(data[0].text.strip(), "FLAG")


def check_degree_level(data):
    if int(data[0].text.strip()) not in range(0, 3):
        warnings.warn("Code does not map to an expected value")
        return "FLAG", "FLAG"
    else:
        if int(data[0].text.strip()) == 0:
            raise ProcessDataError("Received undergraduate work, degree level is 0")
        elif int(data[0].text.strip()) == 1:
            return "Masters", "Masters Thesis"
        elif int(data[0].text.strip()) == 2:
            return "Doctoral", "Dissertation"


def csv_exporter(data, path, new_bagit_directory, files_path):
    """Creates csv with metadata information"""

    columns = [
        "source_identifier",
        "model",
        "title",
        "creator",
        "subject",
        "description",
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

    data_path = new_bagit_directory + "/data"
    files = []
    file_string = ""

    # Find pdf files required for upload to hyrax while ignoring other directories
    for file in os.listdir(data_path):
        if (
            os.path.join(file) != "meta"
            and os.path.join(file) != "LAC"
            and os.path.join(file) != "contributor"
        ):
            if os.path.isdir(os.path.join(data_path, file)):
                subdirectory = os.path.join(data_path, file)
                for subfile in os.listdir(subdirectory):
                    files.append(subfile)
                    shutil.copyfile(
                        os.path.join(subdirectory, subfile),
                        os.path.join(files_path, subfile),
                    )
            else:
                files.append(file)
                shutil.copyfile(
                    os.path.join(data_path, file), os.path.join(files_path, file)
                )

    for i in range(len(files)):
        file_string += files[i]
        if i < (len(files) - 1):
            file_string += " | "

    if not files:
        raise MissingFile("Missing PDF files")
    rows = []

    rows.append(data.source_identifier)
    rows.append("Etd")
    rows.append(data.title)
    rows.append(data.creator)
    rows.append(data.pro_subject)
    rows.append(data.description)
    rows.append(data.publisher)
    rows.append(data.contributor)
    rows.append(data.date_created)
    rows.append(data.language)
    rows.append(data.name)
    rows.append(data.discipline)
    rows.append(data.level)
    rows.append(data.resource_type)
    rows.append(file_string)

    if os.path.isfile(path + "/metadata.csv"):
        with open(path + "/metadata.csv", "a", newline="") as csvfile:
            metadatawriter = csv.writer(
                csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL
            )
            metadatawriter.writerow(rows)
            click.echo(f"Updated csv metadata with {os.path.basename(path)} data")
    else:
        with open(path + "/metadata.csv", "w", newline="") as csvfile:
            metadatawriter = csv.writer(
                csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL
            )
            metadatawriter.writerow(columns)
            metadatawriter.writerow(rows)
            click.echo(f"Created csv metadata for {os.path.basename(path)}")


def get_work_id(target, package, remove_packages):
    headers = {"Content-type": "application/json", "Token": "12345"}
    get_response = requests.get(
        target + "/catalog.json?”sourcetesim”=" + package.source_identifier,
        headers=headers,
    )

    # Check if the get response received information
    if get_response.status_code == 200:
        work_json_data = get_response.json()
        work_id = ""
        for i in range(len(work_json_data["response"]["docs"])):
            if (
                work_json_data["response"]["docs"][i]["source_tesim"][0]
                == package.source_identifier
            ):
                work_id = work_json_data["response"]["docs"][i]["id"]

        # If work id was not found, then assume import failed
        if work_id == "":
            click.echo(
                f"Couldn't find work id for {package.source_identifier}, import for this work failed"
            )
            log_message = f"{package.source_identifier} | Error retrieving work id\nProcessed on: {str(datetime.date.today())}"
            log_failed_array.append(log_message)
            # Add the identifier of the package that failed to list for processing
            remove_packages.append(package.source_identifier)
        return work_id, remove_packages

    else:
        click.echo(
            "Error occurred when attempting to retrieve work id of package. Logging as failed package..."
        )
        log_message = f"{package.source_identifier} | Error retrieving work id\nProcessed on: {str(datetime.date.today())}"
        log_failed_array.append(log_message)
        raise click.Abort


def create_crossref_data(package, identifier, work_link_dict):
    # Check for mononymous names
    mononymous = False
    split_name = package.creator.split(",")
    if len(split_name) < 2:
        mononymous = True

    surname = split_name[0]

    if not mononymous:
        given_name = split_name[1].strip()

    # Get the full degree name from the abbreviated one
    degree_name = package.name

    # Create the tuple for crossref data
    crossref_data = CrossRefData(
        given_name=given_name,
        surname=surname,
        title=package.title,
        approval_date=package.date_created,
        degree=degree_name,
        identifier=identifier,
        resource=work_link_dict[package.source_identifier],
    )
    return crossref_data


def create_crossref(crossref_data, crossref_path, running_file):

    if not os.path.isfile(crossref_path):
        click.echo(f"{running_file} does not exist yet. Creating now...")

        tree = ET.ElementTree()
        doi_batch = ET.Element(
            "doi_batch",
            attrib={
                "version": "4.4.1",
                "xmlns": "http://www.crossref.org/schema/4.4.1",
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "xsi:schemaLocation": "http://www.crossref.org/schema/4.4.1 http://www.crossref.org/schemas/crossref4.4.1.xsd",
            },
        )
        tree._setroot(doi_batch)

        # Header Data
        head = ET.SubElement(doi_batch, "head")
        doi_batch_id = ET.SubElement(head, "doi_batch_id")
        doi_batch_id.text = str(int(time.time()))
        timestamp = ET.SubElement(head, "timestamp")
        timestamp.text = f"{time.time()*1e7:.0f}"

        depositor = ET.SubElement(head, "depositor")
        depositor_name = ET.SubElement(depositor, "depositor_name")
        depositor_name.text = "Carleton University Library"
        email_address = ET.SubElement(depositor, "email_address")
        email_address.text = "doi@library.carleton.ca"

        registrant = ET.SubElement(head, "registrant")
        registrant.text = "Carleton University"
        body = ET.SubElement(doi_batch, "body")
        dissertation = ET.SubElement(body, "dissertation")

    else:
        ET.register_namespace("version", "4.4.1")
        ET.register_namespace("", "http://www.crossref.org/schema/4.4.1")
        ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance.1")
        ET.register_namespace(
            "schemaLocation",
            "http://www.crossref.org/schema/4.4.1 http://www.crossref.org/schemas/crossref4.4.1.xsd",
        )

        # Retrieves data from the unformatted XML file
        tree = ET.parse(running_file)
        doi_batch = tree.getroot()
        body = doi_batch.findall("{http://www.crossref.org/schema/4.4.1}body")[0]
        dissertation = ET.SubElement(body, "dissertation")

    # Body Data
    dissertation = ET.Element("dissertation")
    person_name = ET.SubElement(
        dissertation,
        "person_name",
        attrib={"contributor_role": "author", "sequence": "first"},
    )
    given_name = ET.SubElement(person_name, "given_name")
    given_name.text = crossref_data.given_name
    surname = ET.SubElement(person_name, "surname")
    surname.text = crossref_data.surname

    titles = ET.SubElement(dissertation, "titles")
    title = ET.SubElement(titles, "title")
    title.text = crossref_data.title
    approval_date = ET.SubElement(
        dissertation, "approval_date", attrib={"media_type": "online"}
    )
    year = ET.SubElement(approval_date, "year")
    year.text = crossref_data.approval_date

    institution = ET.SubElement(dissertation, "institution")
    institution_name = ET.SubElement(institution, "institution_name")
    institution_name.text = "Carleton University"
    institution_place = ET.SubElement(institution, "institution_place")
    institution_place.text = "Ottawa, Ontario"

    degree = ET.SubElement(dissertation, "degree")
    degree.text = crossref_data.degree

    # If no identifier was provided, then use the most recent identifier as the base for the newest entry
    if crossref_data.identifier == -1:
        diss_entry = body.findall("{http://www.crossref.org/schema/4.4.1}dissertation")[
            len(body.findall("{http://www.crossref.org/schema/4.4.1}dissertation")) - 1
        ]
        doi_link = diss_entry.findall(".//{http://www.crossref.org/schema/4.4.1}doi")[
            0
        ].text
        temp = doi_link.split("/")[2]
        identifier = int(temp.split("-")[1]) + 1

    else:
        identifier = crossref_data.identifier

    doi_data = ET.SubElement(dissertation, "doi_data")
    doi = ET.SubElement(doi_data, "doi")
    doi.text = (
        DOI_PREFIX + "/etd/" + crossref_data.approval_date + "-" + str(identifier)
    )
    resource = ET.SubElement(doi_data, "resource")
    resource.text = crossref_data.resource

    body.append(dissertation)

    if os.path.isfile(crossref_path):
        os.remove(crossref_path)

    if os.path.isfile(running_file):
        click.echo(
            f"Crossref entries: {str(int(len(body.findall('{http://www.crossref.org/schema/4.4.1}dissertation'))/2))}"
        )

    crossref_xml = minidom.parseString(
        ET.tostring(doi_batch, encoding="unicode")
    ).toprettyxml(indent="  ", encoding="UTF-8")

    tree.write(running_file, encoding="UTF-8", xml_declaration=True)
    with open(crossref_path, "wb") as file:
        file.write(crossref_xml)

    return doi.text


def create_marc_record(package_name, marc_path, work_link, xml_data):
    """
    Create a MARC encoded record for an ETD package
    """
    processed_title = ""
    subtitle = ""

    if ":" in xml_data.title:
        split_title = xml_data.title.split(":", 1)
        processed_title = split_title[0].strip() + " :"
        subtitle = split_title[1].strip()
        if subtitle[-1] != ".":
            subtitle = subtitle + "."
    else:
        processed_title = xml_data.title.strip()
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

    processed_author = xml_data.creator.strip()
    if processed_author[-1] != "-":
        processed_author = processed_author + ","

    try:
        with open(
            os.path.join(marc_path, package_name + "_marc.mrc"), "wb"
        ) as marc_file:

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
                        today.strftime("%y%m%d"), xml_data.date_created
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
                    subfields=["a", "Ottawa,", "c", xml_data.date_created],
                )
            )
            record.add_field(
                pymarc.Field(
                    tag="264",
                    indicators=[" ", "4"],
                    subfields=["c", "\u00A9" + xml_data.date_created],
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
                        + xml_data.name
                        + ") - Carleton University, "
                        + xml_data.date_created
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
                        "Licensed through author open access agreement. Commercial use prohibited without author's consent.",
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
            for subject in xml_data.lc_subject:
                record.add_field(
                    pymarc.Field(tag="650", indicators=[" ", "0"], subfields=subject[0])
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
                        xml_data.discipline + ".",
                    ],
                )
            )
            record.add_field(
                pymarc.Field(
                    tag="856",
                    indicators=["4", "0"],
                    subfields=["u", work_link, "z", "Free Access (CURVE Full Text)"],
                )
            )
            record.add_field(
                pymarc.Field(
                    tag="979",
                    indicators=[" ", " "],
                    subfields=[
                        "a",
                        "MARC file generated {} on ETD Processor".format(
                            today.isoformat()
                        ),
                        "9",
                        "LOCAL",
                    ],
                )
            )
            marc_file.write(record.as_marc())
            return marc_path

    except MarcError as e:
        message = f"Unable to create marc file for {package_name}: {e}"
        click.echo(message)
        log_failed_array.append(package_name + " | Marc Error")
    # except MarcError as e:


def update_metadata(metadata_path, doi_link, in_progress_path, target):
    # Update the metadata csv with DOI Link
    headers = {"Content-type": "application/json", "Token": "12345"}
    new_rows = []

    # Remove the file column from metadata file
    with open(metadata_path, "r", newline="") as readfile:
        csv_reader = csv.reader(readfile, delimiter=",")
        for row in csv_reader:
            row.pop()
            if row[0] == "source_identifier":
                row.append("identifier")
            else:
                row.append(doi_link.get(row[0]))
            new_rows.append(row)

    updated_metadata = in_progress_path + "/updated_metadata.csv"

    # Add the doi link column to the metadata file
    with open(updated_metadata, "w", newline="") as csvfile:
        metadatawriter = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for rows in new_rows:
            metadatawriter.writerow(rows)

    # Find the importer id required for updating the metadata of the thesis
    importer_response = requests.get(target + "/importers", headers=headers)

    importer_data = importer_response.json()

    for i in range(0, len(importer_data)):
        # NOTE: CSV_Import name is based on the name provided in initial import to hyrax
        if importer_data[i]["name"] == "CSV_Import":
            importer_id = importer_data[i]["id"]

    return updated_metadata, importer_id


def email_report(marc_path, host, port, source, destination):
    """Prepares email message to be sent"""

    subject = f"ETD Depositor Report - {len(log_success_array)} processed, {len(log_failed_array)} failed"
    message = f"Number of MARC Records: {len(os.listdir(marc_path))} \n"

    message += f"{len(log_success_array)} successful packages:\n--------------\n\n"
    for log in log_success_array:
        message += f"Package: {log} \n\n"

    message += f"{len(log_failed_array)} failed packages:\n--------------\n\n"
    for log in log_failed_array:
        message += f"Package: {log} \n\n"

    send_email(subject, message, host, port, source, destination)


def send_email(subject, body, host, port, source, destination):

    message = f"From: {source}\nTo: {destination}\nSubject: {subject}\n{body}"

    server = smtplib.SMTP(host, port)
    server.sendmail(source, destination, message)
    server.quit()


if __name__ == "__main__":
    etddepositor(obj={})

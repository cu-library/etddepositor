import bagit
import click
import collections
import csv
import datetime
import glob
import grp
import os
import os.path
import pwd
import pymarc
import requests
import shutil
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

# MARC_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are used to create marc records
MARC_SUBDIR = "marc"

# NAMESPACES is a dictionary of namespace prefixes to URIs.
NAMESPACES = {"dc": "http://purl.org/dc/elements/1.1/"}


# CONTEXT_SETTINGS is a click-specific config dict which allows us to define a
# prefix for the automatic environment variable option feature.
CONTEXT_SETTINGS = {"auto_envvar_prefix": "ETD_DEPOSITOR"}

ETDPackageData = collections.namedtuple(
    "ETDPackageData",
    [
        "title",
        "creator",
        "pro_subject",
        "lc_subject",
        "description",
        "publisher",
        "contributor",
        "name",
        "discipline",
        "date",
        "year",
        "language",
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


class PermissionsInvalid(Exception):
    pass


class StillInEmbargo(PermissionsInvalid):
    pass


class RequiredAgreementNotSigned(PermissionsInvalid):
    pass


class UnexpectedLine(PermissionsInvalid):
    pass


class MetadataInvalid(Exception):
    pass


class MissingElementTag(MetadataInvalid):
    pass


class FailedImport(Exception):
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
@click.option("--filetype", type=str, default="zip")
def copy(ctx, inbox, filetype):
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
        for filepath in glob.glob(os.path.join(inbox, f"*.{filetype}"))
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
            print(f"Unable to extract {filepath}: {e}")
        click.echo(f"Done")

    # TODO: Cleanup file permissions after extract.


@etddepositor.command()
@click.pass_context
@click.option(
    "--importer",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.option("--invalid-ok/--invalid-not-ok", default=False)
@click.option("--identifier", type=int, default=-1)
@click.option("--target", type=str, required=True)
def process(ctx, importer, identifier, target, invalid_ok=False):
    """
    Find the oldest unprocessed ETD package and process it.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the three processing location subdirectory paths.
    awaiting_work_path = os.path.join(processing_directory, AWAITING_WORK_SUBDIR)
    in_progress_path = os.path.join(processing_directory, IN_PROGRESS_SUBDIR)
    complete_path = os.path.join(processing_directory, COMPLETE_SUBDIR)

    # Do the 'in progress' and 'complete' directories exist?
    if not os.path.isdir(in_progress_path):
        click.echo(f"{in_progress_path} does not exist yet. Creating now...")
        os.mkdir(in_progress_path, mode=0o770)
    if not os.path.isdir(complete_path):
        click.echo(f"{complete_path} does not exist yet. Creating now...")
        os.mkdir(complete_path, mode=0o770)

    # Naming of XML files for DOIs
    running_file = os.path.join(
        in_progress_path, str(datetime.date.today()) + "-running.xml"
    )
    crossref_file = str(datetime.date.today()) + "-crossref.xml"
    crossref_path = os.path.join(in_progress_path, crossref_file)

    # Checks to see if a crossref XML exists to use the most recent identifier value
    if identifier == -1 and not os.path.isfile(crossref_path):
        click.echo(
            "No existing crossref to obtain identifier, please provide valid one"
        )
        raise click.Abort

    # Get a list of the package directories that are in the awaiting work directory.
    packages_awaiting_work = glob.glob(os.path.join(awaiting_work_path, "*"))

    # Use the old 'less than inf' trick to find the oldest package.
    oldest_etd_package_path = None
    oldest_etd_package_mtime = float("inf")

    for package_path in packages_awaiting_work:
        modified_time = os.path.getmtime(package_path)
        if modified_time < oldest_etd_package_mtime:
            oldest_etd_package_mtime = modified_time
            oldest_etd_package_path = package_path

    if (
        oldest_etd_package_path
        and not invalid_ok
        and not bagit.Bag(oldest_etd_package_path).is_valid()
    ):
        click.echo(
            f"Unable to process {os.path.basename(oldest_etd_package_path)}, BagIt file is not valid."
        )
        oldest_etd_package_path = None

    if oldest_etd_package_path is None:
        click.echo("No valid packages found to process.")
        raise click.Abort

    click.echo(
        f"Moving {os.path.basename(oldest_etd_package_path)} to 'in progress' directory."
    )

    shutil.move(oldest_etd_package_path, in_progress_path)

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
    package_data = extract_metadata(tree.getroot())

    # TODO: Move to completed and clean up file ownership.
    csv_path = csv_exporter(package_data, in_progress_package_path)

    new_bagit = str(datetime.date.today()) + "-" + package_basename
    new_bagit_directory = os.path.join(in_progress_path, new_bagit)

    # Make new directory for bagit to be created and input all data information
    shutil.copytree(in_progress_package_path + "/data", new_bagit_directory)

    # Remove metadata folder and LAC agreement (TODO: Check after for hiding certain files)
    shutil.rmtree(os.path.join(new_bagit_directory, "meta"))
    shutil.rmtree(os.path.join(new_bagit_directory, "LAC"))

    # if os.path.isdir("contributor"):
    #    shutil.rmtree(os.path.join(new_bagit_directory, "contributor"))

    # Create a new bagit containing the metadata.csv and excluding additional data files
    bagit.make_bag(new_bagit_directory, {"Contact-Name": package_data.creator})
    shutil.copyfile(
        in_progress_package_path + "/metadata.csv",
        new_bagit_directory + "/metadata.csv",
    )

    click.echo(
        f"Deleting old {os.path.basename(in_progress_package_path)} from 'in progress' directory."
    )

    shutil.rmtree(in_progress_package_path)
    shutil.make_archive(new_bagit_directory, "zip", new_bagit_directory)

    # import_bagit(importer, complete_path_bagit)
    click.echo(f"Package process complete!")
    click.echo(f"Importing bagit...")

    # Import the bagit package to hyrax
    subprocess.run(
        [
            importer,
            "--name",
            os.path.basename(new_bagit_directory),
            "--parser_klass",
            "Bulkrax::BagitParser",
            "--metadata_file_name",
            "metadata.csv",
            "--metadata_format",
            "Bulkrax::CsvEntry",
            "--commit",
            "Create and Import",
            "--import_file_path",
            new_bagit_directory,
            "--user_id",
            "1",
            "--auth_token",
            "12345",
        ]
    )

    click.echo(f"Getting work id...")
    time.sleep(5)

    # Get work id after import
    headers = {"Content-type": "application/json", "Token": "12345"}
    get_response = requests.get(
        "http://" + target + "/catalog.json?”sourcetesim”=" + package_basename,
        headers=headers,
    )

    work_json_data = get_response.json()

    for i in range(len(work_json_data["response"]["docs"])):
        if work_json_data["response"]["docs"][i]["source_tesim"][0] == package_basename:
            work_id = work_json_data["response"]["docs"][i]["id"]
    print(f"work id:  {work_id} ")

    # raise FailedImport(f"Error has occurred preventing the work from being uploaded successfully")

    work_link = target + "/concern/works/" + work_id

    create_marc_record(
        processing_directory, new_bagit_directory, work_link, package_data
    )

    # Check for mononymous names
    mononymous = False
    split_name = package_data.creator.split(",")
    if len(split_name) < 2:
        print("Mononymous Name")
        mononymous = True

    surname = split_name[0]

    if not mononymous:
        given_name = split_name[1].strip()

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)

    # Get the full degree name from the abbreviated one
    degree_name = list(config_yaml["abbrev"].keys())[
        list(config_yaml["abbrev"].values()).index(package_data.name)
    ]

    crossref_data = CrossRefData(
        given_name=given_name,
        surname=surname,
        title=package_data.title,
        approval_date=package_data.year,
        degree=degree_name,
        identifier=identifier,
        resource=work_link,
    )

    doi_link = create_crossref(crossref_data, crossref_path, running_file)
    click.echo(f"crossref xml created!")

    # Update the metadata csv with DOI Link
    new_column = ["identifier", doi_link]
    new_rows = []

    with open(new_bagit_directory + "/metadata.csv", "r", newline="") as readfile:
        csv_reader = csv.reader(readfile, delimiter=",")
        i = 0
        for row in csv_reader:
            row.append(new_column[i])
            new_rows.append(row)
            i = i + 1

    # Overwrite old bagit with updated metadata
    with open(new_bagit_directory + "/metadata.csv", "w", newline="") as csvfile:
        metadatawriter = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        metadatawriter.writerow(new_rows[0])
        metadatawriter.writerow(new_rows[1])

    click.echo(f"Updated csv metadata for {os.path.basename(new_bagit_directory)}")

    os.remove(new_bagit_directory + ".zip")
    shutil.make_archive(new_bagit_directory, "zip", new_bagit_directory)

    importer_response = requests.get("http://" + target + "/importers", headers=headers)

    importer_data = importer_response.json()

    for i in range(0, len(importer_data)):
        if importer_data[i]["name"] == os.path.basename(new_bagit_directory):
            importer_id = importer_data[i]["id"]

    # Import the bagit package to hyrax
    print("----------------")

    click.echo(f"Re-Importing metadata...")

    """
    subprocess.run(
        [
            importer,
            "--importer_id",
            str(importer_id),
            "--name",
            os.path.basename(new_bagit_directory),
            "--commit",
            "Update and Re-Import (update metadata only)",
            "--import_file_path",
            csv_path,
            "--user_id",
            "1",
            "--auth_token",
            "12345",
        ]
    )
    """

    """
    click.echo(
        f"Moving new bagit {os.path.basename(new_bagit_directory)} to 'complete' directory."
    )
    complete_path_bagit = shutil.move(new_bagit_directory, complete_path)
    """


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
                raise StillInEmbargo(
                    f"The embargo date of {expiry_date} has not passed."
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
                raise RequiredAgreementNotSigned(f"{line} is invalid.")
        else:
            raise UnexpectedLine(
                f"{line} was not expected in the permissions document content."
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


def extract_metadata(root):

    title = root.findall("dc:title", namespaces=NAMESPACES)
    creator = root.findall("dc:creator", namespaces=NAMESPACES)

    if not title:
        raise MissingElementTag(f"title element tag was not found")
    else:
        if title[0].text.strip() == "":
            raise MissingElementTag(f"title element tag was found but was empty")
    if not creator:
        raise MissingElementTag(f"creator element tag was not found")
    else:
        if creator[0].text.strip() == "":
            raise MissingElementTag(f"creator element tag was found but was empty")

    title = title[0].text.strip()
    creator = creator[0].text.strip()

    subject = root.findall("dc:subject", namespaces=NAMESPACES)
    description = root.findall("dc:description", namespaces=NAMESPACES)
    publisher = root.findall("dc:publisher", namespaces=NAMESPACES)
    contributor = root.findall("dc:contributor", namespaces=NAMESPACES)
    date = root.findall("dc:date", namespaces=NAMESPACES)
    language = root.findall("dc:language", namespaces=NAMESPACES)

    pro_subject = check_metadata(subject, "pro_subject")
    lc_subject = check_metadata(subject, "lc_subject")
    description = check_metadata(description, "description")
    publisher = check_metadata(publisher, "publisher")

    if contributor:
        contributor = check_metadata(contributor, "contributor")
    else:
        contributor = ""

    date = check_metadata(date, "date")
    language = check_metadata(language, "language")

    year = date[:4]

    name = root.findall(".//{http://www.ndltd.org/standards/metadata/etdms/1.1/}name")
    discipline = root.findall(
        ".//{http://www.ndltd.org/standards/metadata/etdms/1.1/}discipline"
    )

    name = check_metadata(name, "name")
    discipline = check_metadata(discipline, "discipline")

    print(contributor)

    data = ETDPackageData(
        title=title,
        creator=creator,
        pro_subject=pro_subject,
        lc_subject=lc_subject,
        description=description,
        publisher=publisher,
        contributor=contributor,
        name=name,
        discipline=discipline,
        date=date,
        year=year,
        language=language,
    )

    return data


def check_metadata(data, xml_tag):

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)

    if data[0].text.strip() == "":
        warnings.warn(f"{xml_tag} element tag was empty")

    if xml_tag == "description":
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

    elif xml_tag == "contributor":
        contributor_string = ""
        for i in range(len(data)):
            contributor_string = contributor_string + data[i].text
            if i < (len(data) - 1):
                contributor_string = contributor_string + "; "
        return contributor_string

    elif xml_tag == "pro_subject":
        pro_subject = ""
        for i in range(len(data)):
            pro_subject = pro_subject + config_yaml["proquest_subject"].get(
                data[i].text, ""
            )
            if i < (len(data) - 1):
                pro_subject = pro_subject + "; "
        return pro_subject

    elif xml_tag == "lc_subject":
        lc_subject = []
        for i in range(len(data)):
            lc_subject.append(config_yaml["lc_subject"].get(data[i].text, [["a", ""]]))
        return lc_subject

    elif xml_tag == "language":
        if data[0].text.strip() != "eng" and "fre":
            warnings.warn(f"language tag was not an expected eng or fre tag")
        if data[0].text.strip() == "eng":
            return "English"
        elif data[0].text.strip() == "fre":
            return "French"

    elif xml_tag == "date":
        try:
            date_obj = datetime.datetime.strptime(data[0].text.strip(), "%Y-%M-%d")
        except:
            warnings.warn(f"date tag is not in expected YYYY-MM-DD format")

    elif xml_tag == "name":
        try:
            return config_yaml["abbrev"].get(data[0].text.strip())
        except:
            warnings.warn(f"Missing name tag")

    elif xml_tag == "discipline":
        try:
            return config_yaml["degree_discipline"].get(data[0].text.strip(), "FLAG")
        except:
            warnings.warn(f"Missing degree discipline tag")
    data = data[0].text.strip()
    return data


def csv_exporter(data, path):
    """Creates csv with metadata information"""

    columns = [
        "source_identifier",
        "title",
        "creator",
        "subject",
        "description",
        "publisher",
        "contributor",
        "date",
        "year",
        "language",
    ]
    rows = []

    rows.append(os.path.basename(path))
    rows.append(data.title)
    rows.append(data.creator)
    rows.append(data.pro_subject)
    rows.append(data.description)
    rows.append(data.publisher)
    rows.append(data.contributor)
    rows.append(data.date)
    rows.append(data.year)
    rows.append(data.language)

    with open(path + "/metadata.csv", "w", newline="") as csvfile:
        metadatawriter = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        metadatawriter.writerow(columns)
        metadatawriter.writerow(rows)
    click.echo(f"Created csv metadata for {os.path.basename(path)}")

    return path + "metadata.csv"


@etddepositor.command()
@click.pass_context
# @click.option("--invalid-ok/--invalid-not-ok", default=False)
def signal(path):
    # os.system("ping http://localhost:3000/catalog.json?”sourcetesim”=“101117229_3805")
    headers = {"Content-type": "application/json", "Token": "12345"}
    response = requests.get(
        "http://localhost:3000/catalog.json?”sourcetesim”='100983183_4099'",
        headers=headers,
    )
    # print(response.json())
    data = response.json()

    """
    response = requests.get("http://localhost:3000/importers", headers=headers)
    print(response)
    json_data = response.json()

    print(type(json_data))
    for i in range(0, len(json_data)):
        if json_data[i]["name"] == "2021-07-19-101060031_3686":
            print(json_data[0]["id"])
            """

    # print(type(data["response"]["docs"][0]))
    print(data["response"]["docs"][1]["source_tesim"][0])
    # for i in range(len(data["response"]["docs"][i])):
    #    print()
    if "id" in data["response"]["docs"][0]:
        print(data["response"]["docs"][0]["id"])
    else:
        print("no key")
        raise FailedImport(f"Uploading ETD package has failed")


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
        print(
            "Number of entries in crossref xml: "
            + str(
                int(
                    len(
                        body.findall(
                            "{http://www.crossref.org/schema/4.4.1}dissertation"
                        )
                    )
                    / 2
                )
            )
        )

    crossref_xml = minidom.parseString(
        ET.tostring(doi_batch, encoding="unicode")
    ).toprettyxml(indent="  ", encoding="UTF-8")

    tree.write(running_file, encoding="UTF-8", xml_declaration=True)
    with open(crossref_path, "wb") as file:
        file.write(crossref_xml)

    return doi.text


def import_bagit(importer, bagit_path):

    subprocess.run(
        [
            importer,
            "--name",
            os.path.basename(bagit_path),
            "--parser_klass",
            "Bulkrax::BagitParser",
            "--metadata_file_name",
            "metadata.csv",
            "--metadata_format",
            "Bulkrax::CsvEntry",
            "--commit",
            "Create and Import",
            "--import_file_path",
            bagit_path,
            "--user_id",
            "1",
            "--auth_token",
            "12345",
        ]
    )


def create_marc_record(processing_directory, bagit_path, work_link, xml_data):
    # Create a MARC encoded record for an ETD package

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)

    click.echo(f"Creating MARC record for {os.path.basename(bagit_path)}")

    marc_path = os.path.join(processing_directory, MARC_SUBDIR)
    if not os.path.isdir(marc_path):
        click.echo(f"{marc_path} does not exist yet. Creating now...")
        os.mkdir(marc_path, mode=0o770)

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
            os.path.join(marc_path, os.path.basename(bagit_path) + "_marc.mrc"), "wb"
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
                        today.strftime("%y%m%d"), xml_data.year
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
                    subfields=["a", "Ottawa,", "c", xml_data.year],
                )
            )
            record.add_field(
                pymarc.Field(
                    tag="264",
                    indicators=[" ", "4"],
                    subfields=["c", "\u00A9" + xml_data.year],
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
                        + xml_data.year
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
            # import pdb; pdb.set_trace()
            marc_file.write(record.as_marc())
    except Exception as e:
        print(f"Unable to create marc file for {os.path.basename(bagit_path)}: {e}")

    click.echo(f"MARC record successfully created")


if __name__ == "__main__":
    etddepositor(obj={})
